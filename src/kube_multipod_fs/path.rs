use std::path::{Path, PathBuf};

#[derive(Default, Clone)]
pub struct KubePath {
    pub pod: Option<String>,
    pub container: Option<String>,
    pub path: Option<PathBuf>,
}

impl From<&Path> for KubePath {
    fn from(path: &Path) -> Self {
        let mut p = KubePath::default();

        let mut parts = path.iter();
        if path.is_absolute() {
            parts.next(); // skip the root
        }
        if let Some(pod) = parts.next() {
            p.pod = Some(pod.to_string_lossy().trim_matches('/').to_string());
        }
        if let Some(container) = parts.next() {
            p.container = Some(container.to_string_lossy().trim_matches('/').to_string());
        }
        let path = parts.collect::<PathBuf>();
        if !path.as_os_str().is_empty() {
            p.path = Some(path);
        }

        p
    }
}

#[cfg(test)]
mod test {

    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_from_path() {
        let path = Path::new("/pod/container/path/to/file");
        let p = KubePath::from(path);
        assert_eq!(p.pod, Some("pod".to_string()));
        assert_eq!(p.container, Some("container".to_string()));
        assert_eq!(p.path, Some(PathBuf::from("path/to/file")));

        let path = Path::new("/pod/container");

        let p = KubePath::from(path);
        assert_eq!(p.pod, Some("pod".to_string()));
        assert_eq!(p.container, Some("container".to_string()));

        let path = Path::new("/pod");

        let p = KubePath::from(path);
        assert_eq!(p.pod, Some("pod".to_string()));
        assert!(p.container.is_none());
        assert!(p.path.is_none());

        let path = Path::new("/");

        let p = KubePath::from(path);
        assert!(p.pod.is_none());
        assert!(p.container.is_none());
        assert!(p.path.is_none());
    }

    #[test]
    fn test_relative_path() {
        let path = Path::new("pod/container/path/to/file");
        let p = KubePath::from(path);
        assert_eq!(p.pod, Some("pod".to_string()));
        assert_eq!(p.container, Some("container".to_string()));
        assert_eq!(p.path, Some(PathBuf::from("path/to/file")));

        let path = Path::new("pod/container");

        let p = KubePath::from(path);
        assert_eq!(p.pod, Some("pod".to_string()));
        assert_eq!(p.container, Some("container".to_string()));

        let path = Path::new("pod");

        let p = KubePath::from(path);
        assert_eq!(p.pod, Some("pod".to_string()));
        assert!(p.container.is_none());
        assert!(p.path.is_none());

        let path = Path::new("");

        let p = KubePath::from(path);
        assert!(p.pod.is_none());
        assert!(p.container.is_none());
        assert!(p.path.is_none());
    }
}
