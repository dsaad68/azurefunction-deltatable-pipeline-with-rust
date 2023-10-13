use url::Url;
use std::error::Error;

#[derive(Debug)]
pub struct BlobPath {
    pub account_name: String,
    pub container_name: String,
    pub path: String,
    pub file_name: String,
    pub complete_path: String,
    pub blob_url: String,
    pub abfss_uri:String
}

impl BlobPath {

    pub fn from_blob_url(blob_url: &String) -> Result<Self, Box<dyn Error>> {
        let url = Url::parse(blob_url).map_err(|e| format!("Failed to parse URL: {}", e))?;
        let host = url.host_str().ok_or("Missing host in URL")?;
        let account_name = host.split('.').next().ok_or("Missing account name in host")?.to_string();
        let mut path_segments = url.path_segments().ok_or("Missing path segments in URL")?;
        let container_name = path_segments.next().ok_or("Missing container name in path segments")?.to_string();
        let path = path_segments.clone().collect::<Vec<&str>>().join("/");
        let file_name = path_segments.last().ok_or("Missing file name in path segments")?.to_string();
        
        let mut path = path.trim_end_matches(&file_name).trim_end_matches('/').to_string();
        path = if path.is_empty() {
            path
        } else {
            format!("/{}", path)
        };

        let complete_path = format!("{}/{}", path, file_name);
        let abfss_uri = Self::generate_abfss_uri(&container_name, &account_name, &path, &file_name);

        Ok(BlobPath{
            account_name,
            container_name,
            path,
            file_name,
            complete_path,
            blob_url: blob_url.to_string(),
            abfss_uri
        })

    }

    fn generate_abfss_uri(container_name: &String, account_name: &String, path: &String, file_name: &String) -> String {
        format!(
            "abfss://{}@{}.dfs.core.windows.net/{}{}",
            container_name,
            account_name,
            path,
            file_name
        )
    }
}