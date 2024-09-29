//! ## Kube MultiPod FS
//!
//! The `KubeMultiPodFs` client is a client that allows you to interact with multiple pods in a Kubernetes cluster.

mod path;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Client, Config};
use remotefs::fs::{
    FileType, Metadata, ReadStream, RemoteError, RemoteErrorType, RemoteFs, RemoteResult, UnixPex,
    Welcome, WriteStream,
};
use remotefs::File;
use tokio::runtime::Runtime;

use self::path::KubePath;
use crate::KubeContainerFs;

/// Kube MultiPod FS
///
/// The `KubeMultiPodFs` client is a client that allows you to interact with multiple pods in a Kubernetes cluster.
///
/// Underneath it uses the `KubeContainerFs` client to interact with the pods, but it changes the current pod and
/// the container name under the hood, to simulate a multi-pod filesystem.
///
/// Path are relative to the current pod and container and have the following format:
///
/// /pod-name/container-name/path/to/file
pub struct KubeMultiPodFs {
    kube: KubeContainerFs,
    runtime: Arc<Runtime>,
}

impl KubeMultiPodFs {
    /// Create a new `KubeMultiPodFs` client
    pub fn new(runtime: &Arc<Runtime>) -> Self {
        Self {
            kube: KubeContainerFs::new("", "", runtime),
            runtime: runtime.clone(),
        }
    }

    /// Set configuration
    pub fn config(mut self, config: Config) -> Self {
        self.kube = self.kube.config(config);
        self
    }

    /// Get the current pod name
    fn pod_name(&self) -> Option<&str> {
        if self.kube.pod_name.is_empty() {
            None
        } else {
            Some(&self.kube.pod_name)
        }
    }

    /// Returns the current container name
    fn container_name(&self) -> Option<&str> {
        if self.kube.container.is_empty() {
            None
        } else {
            Some(&self.kube.container)
        }
    }

    /// Dispatch operations based on the path
    ///
    /// The `on_root` closure is called when the path is `/`
    /// The `on_pod` closure is called when the path is `/pod-name`
    /// The `on_container` closure is called when the path is `/pod-name/container-name` or `/pod-name/container-name/path/to/file`
    ///
    /// In any case, the current pod and container are set accordingly.
    fn path_dispatch<T, FR, FP, FC, FPP>(
        &mut self,
        path: KubePath,
        on_root: FR,
        on_pod: FP,
        on_container: FC,
        on_path: FPP,
    ) -> T
    where
        FR: FnOnce(&mut Self) -> T,
        FP: FnOnce(&mut Self, &str) -> T,
        FC: FnOnce(&mut Self, &str) -> T,
        FPP: FnOnce(&mut Self, &Path) -> T,
    {
        if path.pod.is_none() {
            return on_root(self);
        }
        if path.container.is_none() {
            return on_pod(self, path.pod.as_deref().unwrap());
        }

        // temporary set pod and container
        if let Some(p) = path.path {
            let prev_pod = self.kube.pod_name.clone();
            let prev_container = self.kube.container.clone();
            self.kube.pod_name = path.pod.unwrap();
            self.kube.container = path.container.unwrap();
            let res = on_path(self, &p);

            // restore pod and container
            self.kube.pod_name = prev_pod;
            self.kube.container = prev_container;

            res
        } else {
            on_container(self, path.container.as_deref().unwrap())
        }
    }

    /// List pods
    fn list_pods(&self) -> RemoteResult<Vec<File>> {
        let api = self.kube.pods.as_ref().ok_or_else(|| {
            RemoteError::new_ex(
                RemoteErrorType::NotConnected,
                "Not connected to a Kubernetes cluster",
            )
        })?;
        let pods = self
            .runtime
            .block_on(async { api.list(&Default::default()).await })
            .map_err(|err| RemoteError::new_ex(RemoteErrorType::ProtocolError, err))?;

        Ok(pods
            .into_iter()
            .map(|pod| File {
                path: {
                    let mut p = PathBuf::from("/");
                    p.push(pod.metadata.name.unwrap_or_default());
                    p
                },
                metadata: Metadata::default().file_type(FileType::Directory),
            })
            .collect())
    }

    /// List containers
    fn list_containers(&self, pod_name: &str) -> RemoteResult<Vec<File>> {
        let api = self.kube.pods.as_ref().ok_or_else(|| {
            RemoteError::new_ex(
                RemoteErrorType::NotConnected,
                "Not connected to a Kubernetes cluster",
            )
        })?;
        let pod = self
            .runtime
            .block_on(async { api.get(pod_name).await })
            .map_err(|err| RemoteError::new_ex(RemoteErrorType::NoSuchFileOrDirectory, err))?;

        let pod_spec = pod.spec.ok_or_else(|| {
            RemoteError::new_ex(RemoteErrorType::NoSuchFileOrDirectory, "Pod spec not found")
        })?;

        Ok(pod_spec
            .containers
            .into_iter()
            .map(|container| File {
                path: {
                    let mut p = PathBuf::from("/");
                    p.push(pod_name);
                    p.push(&container.name);
                    debug!("found container {} -> {}", container.name, p.display());

                    p
                },
                metadata: Metadata::default().file_type(FileType::Directory),
            })
            .collect())
    }

    /// Stat root
    #[inline]
    fn stat_root(&self) -> RemoteResult<File> {
        Ok(File {
            path: PathBuf::from("/"),
            metadata: Metadata::default().file_type(FileType::Directory),
        })
    }

    /// Stat pod
    fn stat_pod(&self, pod: &str) -> RemoteResult<File> {
        let pods = self.list_pods()?;

        pods.into_iter().find(|f| f.name() == pod).ok_or_else(|| {
            RemoteError::new_ex(
                RemoteErrorType::NoSuchFileOrDirectory,
                format!("Pod {} not found", pod),
            )
        })
    }

    /// Stat container
    fn stat_container(&self, container: &str) -> RemoteResult<File> {
        let pod_name = self.pod_name().ok_or_else(|| {
            RemoteError::new_ex(
                RemoteErrorType::NoSuchFileOrDirectory,
                "No pod to stat container",
            )
        })?;
        let containers = self.list_containers(pod_name)?;

        containers
            .into_iter()
            .find(|f| f.name() == container)
            .ok_or_else(|| {
                RemoteError::new_ex(
                    RemoteErrorType::NoSuchFileOrDirectory,
                    format!("Container {} not found", container),
                )
            })
    }

    /// Check whether pod exists
    fn exists_pod(&self, pod: &str) -> RemoteResult<bool> {
        let api = self.kube.pods.as_ref().ok_or_else(|| {
            RemoteError::new_ex(
                RemoteErrorType::NotConnected,
                "Not connected to a Kubernetes cluster",
            )
        })?;

        Ok(self.runtime.block_on(async { api.get(pod).await.is_ok() }))
    }

    /// Check whether container exists
    fn exists_container(&self, container: &str) -> RemoteResult<bool> {
        let pod_name = self.pod_name().ok_or_else(|| {
            RemoteError::new_ex(
                RemoteErrorType::NoSuchFileOrDirectory,
                "No pod to check container existence",
            )
        })?;

        let api = self.kube.pods.as_ref().ok_or_else(|| {
            RemoteError::new_ex(
                RemoteErrorType::NotConnected,
                "Not connected to a Kubernetes cluster",
            )
        })?;

        let pod = self
            .runtime
            .block_on(async { api.get(pod_name).await })
            .map_err(|err| RemoteError::new_ex(RemoteErrorType::NoSuchFileOrDirectory, err))?;

        let pod_spec = pod.spec.ok_or_else(|| {
            RemoteError::new_ex(RemoteErrorType::NoSuchFileOrDirectory, "Pod spec not found")
        })?;

        Ok(pod_spec.containers.iter().any(|c| c.name == container))
    }
}

impl RemoteFs for KubeMultiPodFs {
    fn connect(&mut self) -> RemoteResult<Welcome> {
        debug!("Initializing Kube connection...");
        let api = self.runtime.block_on(async {
            let client = match self.kube.config.as_ref() {
                Some(config) => Client::try_from(config.clone())
                    .map_err(|err| RemoteError::new_ex(RemoteErrorType::ConnectionError, err)),
                None => Client::try_default()
                    .await
                    .map_err(|err| RemoteError::new_ex(RemoteErrorType::ConnectionError, err)),
            }?;
            let api: Api<Pod> = Api::default_namespaced(client);

            Ok(api)
        })?;

        // Set pods
        self.kube.pods = Some(api);
        debug!("Getting working directory...");

        Ok(Welcome::default())
    }

    fn disconnect(&mut self) -> RemoteResult<()> {
        self.kube.disconnect()
    }

    fn is_connected(&mut self) -> bool {
        if self.pod_name().is_none() {
            self.kube.pods.is_some()
        } else {
            self.kube.is_connected()
        }
    }

    fn pwd(&mut self) -> RemoteResult<PathBuf> {
        let mut p = PathBuf::from("/");

        // compose path in format /pod-name/container-name/pwd
        if let Some(pod_name) = self.pod_name() {
            p.push(pod_name);
        } else {
            return Ok(p);
        }

        if let Some(container_name) = self.container_name() {
            p.push(container_name);
        } else {
            return Ok(p);
        }

        p.push(self.kube.pwd()?);

        Ok(p)
    }

    fn change_dir(&mut self, dir: &Path) -> RemoteResult<PathBuf> {
        let path = KubePath::from(dir);

        let prev_pod = self.pod_name().unwrap_or("").to_string();
        let prev_container = self.container_name().unwrap_or("").to_string();

        if let Some(pod) = path.pod {
            if self.exists_pod(&pod)? {
                self.kube.pod_name = pod.to_string();
            } else {
                return Err(RemoteError::new_ex(
                    RemoteErrorType::NoSuchFileOrDirectory,
                    format!("Pod {} does not exist", pod),
                ));
            }
        }
        if let Some(container) = path.container {
            if self.exists_container(&container)? {
                self.kube.container = container.to_string();
            } else {
                // restore previous pod
                self.kube.pod_name = prev_pod;
                return Err(RemoteError::new_ex(
                    RemoteErrorType::NoSuchFileOrDirectory,
                    format!("Container {} does not exist", container),
                ));
            }
        }
        let res = if let Some(path) = path.path {
            self.kube.change_dir(&path)
        } else {
            self.kube.change_dir(Path::new("/"))
        };

        // restore previous pod and container
        if let Err(err) = res {
            self.kube.pod_name = prev_pod;
            self.kube.container = prev_container;

            return Err(err);
        }

        self.pwd()
    }

    fn list_dir(&mut self, path: &Path) -> RemoteResult<Vec<File>> {
        let path = KubePath::from(path);

        self.path_dispatch(
            path,
            |fs| fs.list_pods(),
            |fs, pod| fs.list_containers(pod),
            |fs, _| fs.kube.list_dir(Path::new("/")),
            |fs, path| fs.kube.list_dir(path),
        )
    }

    fn stat(&mut self, path: &Path) -> RemoteResult<File> {
        let path = KubePath::from(path);

        self.path_dispatch(
            path,
            |fs| fs.stat_root(),
            |fs, pod| fs.stat_pod(pod),
            |fs, container| fs.stat_container(container),
            |fs, path| fs.kube.stat(path),
        )
    }

    fn setstat(&mut self, path: &Path, metadata: Metadata) -> RemoteResult<()> {
        let path = KubePath::from(path);

        self.path_dispatch(
            path,
            |_| Ok(()),
            |_, _| Ok(()),
            |_, _| Ok(()),
            |fs, path| fs.kube.setstat(path, metadata),
        )
    }

    fn exists(&mut self, path: &Path) -> RemoteResult<bool> {
        let path = KubePath::from(path);

        self.path_dispatch(
            path,
            |_| Ok(true),
            |fs, pod| fs.exists_pod(pod),
            |fs, container| fs.exists_container(container),
            |fs, path| fs.kube.exists(path),
        )
    }

    fn remove_file(&mut self, path: &Path) -> RemoteResult<()> {
        let path = KubePath::from(path);

        self.path_dispatch(
            path,
            |_| Err(RemoteError::new(RemoteErrorType::CouldNotRemoveFile)),
            |_, _| Err(RemoteError::new(RemoteErrorType::CouldNotRemoveFile)),
            |_, _| Err(RemoteError::new(RemoteErrorType::CouldNotRemoveFile)),
            |fs, path| fs.kube.remove_file(path),
        )
    }

    fn remove_dir(&mut self, path: &Path) -> RemoteResult<()> {
        let path = KubePath::from(path);

        self.path_dispatch(
            path,
            |_| Err(RemoteError::new(RemoteErrorType::CouldNotRemoveFile)),
            |_, _| Err(RemoteError::new(RemoteErrorType::CouldNotRemoveFile)),
            |_, _| Err(RemoteError::new(RemoteErrorType::CouldNotRemoveFile)),
            |fs, path| fs.kube.remove_dir(path),
        )
    }

    fn remove_dir_all(&mut self, path: &Path) -> RemoteResult<()> {
        let path = KubePath::from(path);

        self.path_dispatch(
            path,
            |_| Err(RemoteError::new(RemoteErrorType::CouldNotRemoveFile)),
            |_, _| Err(RemoteError::new(RemoteErrorType::CouldNotRemoveFile)),
            |_, _| Err(RemoteError::new(RemoteErrorType::CouldNotRemoveFile)),
            |fs, path| fs.kube.remove_dir_all(path),
        )
    }

    fn create_dir(&mut self, path: &Path, mode: UnixPex) -> RemoteResult<()> {
        let path = KubePath::from(path);

        self.path_dispatch(
            path,
            |_| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |fs, path| fs.kube.create_dir(path, mode),
        )
    }

    fn symlink(&mut self, path: &Path, target: &Path) -> RemoteResult<()> {
        let path = KubePath::from(path);

        self.path_dispatch(
            path,
            |_| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |fs, path| fs.kube.symlink(path, target),
        )
    }

    fn copy(&mut self, src: &Path, dest: &Path) -> RemoteResult<()> {
        let path = KubePath::from(src);

        self.path_dispatch(
            path,
            |_| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |fs, path| fs.kube.copy(path, dest),
        )
    }

    fn mov(&mut self, src: &Path, dest: &Path) -> RemoteResult<()> {
        let path = KubePath::from(src);

        self.path_dispatch(
            path,
            |_| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |fs, path| fs.kube.mov(path, dest),
        )
    }

    fn exec(&mut self, cmd: &str) -> RemoteResult<(u32, String)> {
        if self.pod_name().is_none() || self.container_name().is_none() {
            return Err(RemoteError::new_ex(
                RemoteErrorType::ProtocolError,
                "No pod or container to execute command on",
            ));
        }

        self.kube.exec(cmd)
    }

    fn append(&mut self, _path: &Path, _metadata: &Metadata) -> RemoteResult<WriteStream> {
        Err(RemoteError::new(RemoteErrorType::UnsupportedFeature))
    }

    fn create(&mut self, _path: &Path, _metadata: &Metadata) -> RemoteResult<WriteStream> {
        Err(RemoteError::new(RemoteErrorType::UnsupportedFeature))
    }

    fn open(&mut self, _path: &Path) -> RemoteResult<ReadStream> {
        Err(RemoteError::new(RemoteErrorType::UnsupportedFeature))
    }

    fn create_file(
        &mut self,
        path: &Path,
        metadata: &Metadata,
        reader: Box<dyn std::io::Read>,
    ) -> RemoteResult<u64> {
        let path = KubePath::from(path);

        self.path_dispatch(
            path,
            |_| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |fs, path| fs.kube.create_file(path, metadata, reader),
        )
    }

    fn append_file(
        &mut self,
        path: &Path,
        metadata: &Metadata,
        reader: Box<dyn std::io::Read>,
    ) -> RemoteResult<u64> {
        let path = KubePath::from(path);

        self.path_dispatch(
            path,
            |_| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |fs, path| fs.kube.append_file(path, metadata, reader),
        )
    }

    fn open_file(&mut self, src: &Path, dest: Box<dyn std::io::Write + Send>) -> RemoteResult<u64> {
        let path = KubePath::from(src);

        self.path_dispatch(
            path,
            |_| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |_, _| {
                Err(RemoteError::new_ex(
                    RemoteErrorType::CouldNotOpenFile,
                    "This operation requires a pod and a container",
                ))
            },
            |fs, path| fs.kube.open_file(path, dest),
        )
    }
}
