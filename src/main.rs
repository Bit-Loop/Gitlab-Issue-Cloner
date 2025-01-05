// Simple Gitlab-Issue Scraper
// By Isaiah Jackson 
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use config::Config;
use regex::Regex;
use ctrlc;
use std::io::Write;
use std::io::{BufWriter, Seek, SeekFrom};

const SAVE_BATCH_SIZE: usize = 10000;
const MIN_BATCH_SIZE: u32 = 50;
const MAX_BATCH_SIZE: u32 = 1000;
const INDEX_BATCH_SIZE: usize = 1000;

#[derive(Debug)]
struct GitlabError(String);

impl std::error::Error for GitlabError {}

impl std::fmt::Display for GitlabError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Box<dyn std::error::Error>> for GitlabError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        GitlabError(error.to_string())
    }
}

impl From<reqwest::Error> for GitlabError {
    fn from(error: reqwest::Error) -> Self {
        GitlabError(error.to_string())
    }
}

impl From<std::io::Error> for GitlabError {
    fn from(error: std::io::Error) -> Self {
        GitlabError(error.to_string())
    }
}

impl From<serde_json::Error> for GitlabError {
    fn from(error: serde_json::Error) -> Self {
        GitlabError(error.to_string())
    }
}

#[derive(Debug, Deserialize, Clone)]
struct Settings {
    default_project: String,
    data_dir: String,
    initial_retry_delay: u64,
    max_retry_delay: u64,
    rate_limit_reset_duration: u64,
    polling_interval: u64,
    per_page: u32,
    max_concurrent_requests: u32,
}

impl Settings {
    fn new() -> Result<Self, Box<dyn Error>> {
        let builder = config::Config::builder()
            .add_source(config::File::with_name("Settings"));
        
        let settings = builder.build()?;
        settings.try_deserialize().map_err(|e| e.into())
    }
}

#[derive(Serialize, Deserialize, Default)]
struct ProgramState {
    last_updated: Option<String>,
    last_issue_id: Option<i64>,
    total_issues: usize,
    initial_run_complete: bool,
}

impl ProgramState {
    fn load(data_dir: &str) -> Result<Self, Box<dyn Error>> {
        let path = Path::new(data_dir).join("state.json");
        if path.exists() {
            let content = fs::read_to_string(path)?;
            Ok(serde_json::from_str(&content)?)
        } else {
            Ok(Self::default())
        }
    }

    fn save(&self, data_dir: &str) -> Result<(), Box<dyn Error>> {
        let path = Path::new(data_dir).join("state.json");
        let content = serde_json::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}

#[derive(Debug)]
struct RateLimit {
    remaining: u32,
    reset_time: Option<Instant>,
    retry_after: Option<Duration>,
}

impl RateLimit {
    fn from_headers(headers: &reqwest::header::HeaderMap) -> Self {
        let remaining = headers
            .get("RateLimit-Remaining")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let reset_time = headers
            .get("RateLimit-Reset")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(|timestamp| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                if timestamp > now {
                    let duration = timestamp - now;
                    println!("Rate limit will reset in {} minutes {} seconds", 
                        duration / 60, duration % 60);
                    Instant::now() + Duration::from_secs(duration)
                } else {
                    Instant::now()
                }
            });

        let retry_after = headers
            .get("Retry-After")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs);

        RateLimit {
            remaining,
            reset_time,
            retry_after,
        }
    }
}

async fn fetch_with_retry(
    client: &Client,
    url: &str,
    query: &[(String, String)],
    settings: &Settings,
) -> Result<reqwest::Response, GitlabError> {
    let mut delay = settings.initial_retry_delay;
    let mut consecutive_429s = 0;

    loop {
        let response = client.get(url).query(query).send().await?;
        let rate_limit = RateLimit::from_headers(response.headers());
        
        // Only warn if rate limit is actually low and not just unset (0)
        if rate_limit.remaining <= 10 && rate_limit.reset_time.is_some() {
            println!("WARNING: Rate limit nearly exhausted ({} remaining)", rate_limit.remaining);
        }
        
        match response.status() {
            status if status.is_success() => {
                consecutive_429s = 0;
                // Reduce delay between successful requests to 100ms
                tokio::time::sleep(Duration::from_millis(100)).await;
                return Ok(response)
            },
            status if status.as_u16() == 429 => {
                consecutive_429s += 1;
                println!("Rate limited! (429 received {} times)", consecutive_429s);
                
                let wait_duration = if let Some(retry_after) = rate_limit.retry_after {
                    println!("Server specified retry after: {:?}", retry_after);
                    retry_after + Duration::from_secs(1) // Add 1 second buffer
                } else if let Some(reset_time) = rate_limit.reset_time {
                    let duration = reset_time.duration_since(Instant::now());
                    println!("Using rate limit reset time");
                    duration + Duration::from_secs(2) // Add 2 seconds buffer
                } else {
                    let exponential_delay = delay * 2u64.pow(consecutive_429s.min(3) as u32);
                    println!("Using exponential backoff: {} seconds", exponential_delay);
                    Duration::from_secs(exponential_delay.min(settings.max_retry_delay))
                };

                println!("Waiting for {:?} before retry...", wait_duration);
                tokio::time::sleep(wait_duration).await;
                continue;
            },
            status if status.is_server_error() => {
                println!("Server error ({}). Retrying in {} seconds...", status, delay);
                tokio::time::sleep(Duration::from_secs(delay)).await;
                delay = (delay * 2).min(settings.max_retry_delay);
                continue;
            }
            status => {
                return Err(GitlabError(format!("HTTP error: {}", status)));
            }
        }
    }
}

async fn fetch_issues(
    client: &Client,
    project_path: &str,
    settings: &Settings,
    running: &Arc<AtomicBool>,  // Add running flag parameter
) -> Result<(Vec<Value>, ProgramState), GitlabError> {
    let mut state = ProgramState::load(&settings.data_dir)?;
    let mut all_issues = Vec::with_capacity(SAVE_BATCH_SIZE); // Pre-allocate memory
    let mut page = 1;
    let mut total_count = 0;

    // Validate and adjust batch size
    let concurrent_batch_size = settings.max_concurrent_requests.clamp(MIN_BATCH_SIZE, MAX_BATCH_SIZE);
    
    // Determine fetch mode
    let is_initial_run = !state.initial_run_complete;
    println!("{}", if is_initial_run {
        "Performing initial fetch of all issues (oldest first)...".to_string()
    } else {
        format!("Checking for new issues since last update: {:?}", state.last_updated)
    });

    let start_time = Instant::now();
    
    loop {
        if !running.load(Ordering::SeqCst) {
            println!("Shutdown requested, saving current progress...");
            if !all_issues.is_empty() {
                export_issues_to_json(&settings.data_dir, &all_issues)?;
                process_wordlist(&settings.data_dir, &all_issues)?;
                
                // Update state before exit
                if is_initial_run {
                    if let Some(updated_at) = all_issues.last()
                        .and_then(|issue| issue["updated_at"].as_str()) {
                        state.last_updated = Some(updated_at.to_string());
                    }
                } else if let Some(updated_at) = all_issues.iter()
                    .filter_map(|issue| issue["updated_at"].as_str())
                    .max() {
                    state.last_updated = Some(updated_at.to_string());
                }
                state.total_issues += all_issues.len();
                state.save(&settings.data_dir)?;
            }
            break;
        }

        let mut concurrent_batch = Vec::with_capacity(concurrent_batch_size as usize);
        
        for _ in 0..concurrent_batch_size {
            let mut query = vec![
                ("page".to_string(), page.to_string()),
                ("per_page".to_string(), settings.per_page.to_string()),
                ("state".to_string(), "all".to_string()),
            ];

            // Different query parameters based on fetch mode
            if is_initial_run {
                query.extend_from_slice(&[
                    ("order_by".to_string(), "created_at".to_string()),
                    ("sort".to_string(), "asc".to_string()),
                ]);
            } else {
                query.extend_from_slice(&[
                    ("order_by".to_string(), "updated_at".to_string()),
                    ("sort".to_string(), "desc".to_string()),
                ]);
                if let Some(ref last_updated) = state.last_updated {
                    query.push(("updated_after".to_string(), last_updated.to_string()));
                }
            }

            let url = format!(
                "https://gitlab.com/api/v4/projects/{}/issues",
                urlencoding::encode(project_path)
            );

            let client = client.clone();
            let settings = settings.clone();
            let url_clone = url.clone();
            
            concurrent_batch.push(tokio::spawn(async move {
                fetch_with_retry(&client, &url_clone, &query, &settings).await
            }));
            
            page += 1;
        }

        let mut batch_empty = true;
        let mut batch_count = 0;
        for task in concurrent_batch {
            if let Ok(response) = task.await {
                match response {
                    Ok(response) => {
                        let resp: Vec<Value> = response.json().await
                            .map_err(|e| GitlabError(e.to_string()))?;
                        batch_count += resp.len();
                        if !resp.is_empty() {
                            batch_empty = false;
                            all_issues.extend(resp);
                        }
                    }
                    Err(e) => {
                        println!("Error in batch request: {}", e);
                        continue;
                    }
                }
            }
        }

        total_count += batch_count;
        println!("Fetched batch {} with {} issues (Total: {})", 
                 page / concurrent_batch_size as i32, 
                 batch_count,
                 total_count);

        if batch_empty {
            break;
        }

        // Save periodically with progress info
        if all_issues.len() >= SAVE_BATCH_SIZE {
            let elapsed = start_time.elapsed();
            let rate = total_count as f64 / elapsed.as_secs_f64();
            println!(
                "Saving batch of {} issues... ({:.1} issues/sec, elapsed: {:.1}s)", 
                all_issues.len(),
                rate,
                elapsed.as_secs_f64()
            );
            
            export_issues_to_json(&settings.data_dir, &all_issues)
                .map_err(|e| GitlabError(format!("Failed to save issues: {}", e)))?;
            process_wordlist(&settings.data_dir, &all_issues)
                .map_err(|e| GitlabError(format!("Failed to update wordlist: {}", e)))?;
            
            all_issues.clear();
            all_issues.shrink_to(SAVE_BATCH_SIZE); // Maintain capacity for next batch
        }

        // Update state periodically
        state.total_issues = total_count;
        state.save(&settings.data_dir)?;
    }

    // Final stats
    let elapsed = start_time.elapsed();
    let rate = total_count as f64 / elapsed.as_secs_f64();
    println!(
        "Fetch complete: {} issues in {:.1}s ({:.1} issues/sec)", 
        total_count,
        elapsed.as_secs_f64(),
        rate
    );

    // Final save of any remaining issues
    if !all_issues.is_empty() {
        export_issues_to_json(&settings.data_dir, &all_issues)?;
        process_wordlist(&settings.data_dir, &all_issues)?;
    }

    if !all_issues.is_empty() {
        if is_initial_run {
            // On initial run, use the latest issue's timestamp
            if let Some(updated_at) = all_issues.last()
                .and_then(|issue| issue["updated_at"].as_str()) {
                state.last_updated = Some(updated_at.to_string());
                println!("Initial run complete. Set last_updated to: {}", updated_at);
            }
            state.initial_run_complete = true;
        } else {
            // For incremental updates, use the most recent timestamp
            if let Some(updated_at) = all_issues.iter()
                .filter_map(|issue| issue["updated_at"].as_str())
                .max() {
                state.last_updated = Some(updated_at.to_string());
                println!("Updated last_updated to: {}", updated_at);
            }
        }
        
        if let Some(id) = all_issues[0]["id"].as_i64() {
            state.last_issue_id = Some(id);
        }
    }

    state.total_issues += all_issues.len();
    state.save(&settings.data_dir)?;
    
    Ok((all_issues, state))
}

struct IssueStore {
    data_dir: String,
    issue_file: BufWriter<fs::File>,
    index: HashSet<i64>,
}

impl IssueStore {
    fn new(data_dir: &str) -> Result<Self, Box<dyn Error>> {
        let json_path = Path::new(data_dir).join("issues.json");
        let index_path = Path::new(data_dir).join("issues.index");
        
        // Load or create index
        let index = if index_path.exists() {
            let content = fs::read_to_string(&index_path)?;
            content.lines()
                .filter_map(|line| line.parse::<i64>().ok())
                .collect()
        } else {
            HashSet::new()
        };

        // Open file in append mode
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&json_path)?;

        // Create if doesn't exist and ensure valid JSON
        if file.metadata()?.len() == 0 {
            let mut writer = BufWriter::new(file);
            writer.write_all(b"[\n")?;
            writer.flush()?;
            Ok(Self {
                data_dir: data_dir.to_string(),
                issue_file: writer,
                index,
            })
        } else {
            Ok(Self {
                data_dir: data_dir.to_string(),
                issue_file: BufWriter::new(file),
                index,
            })
        }
    }

    fn add_issues(&mut self, issues: &[Value]) -> Result<usize, Box<dyn Error>> {
        let mut added = 0;

        for issue in issues {
            if let Some(id) = issue["id"].as_i64() {
                if !self.index.contains(&id) {
                    // Add comma if not first entry
                    if !self.index.is_empty() {
                        self.issue_file.write_all(b",\n")?;
                    }
                    
                    let json = serde_json::to_string(issue)?;
                    self.issue_file.write_all(json.as_bytes())?;
                    self.index.insert(id);
                    added += 1;

                    // Periodically flush to disk
                    if added % INDEX_BATCH_SIZE == 0 {
                        self.issue_file.flush()?;
                        self.save_index()?;
                    }
                }
            }
        }

        if added > 0 {
            self.issue_file.flush()?;
            self.save_index()?;
        }

        println!("Added {} new issues", added);
        Ok(added)
    }

    fn save_index(&self) -> Result<(), Box<dyn Error>> {
        let index_path = Path::new(&self.data_dir).join("issues.index");
        let mut indices: Vec<_> = self.index.iter().collect();
        indices.sort_unstable();
        fs::write(index_path, indices.iter().map(|&id| id.to_string()).collect::<Vec<_>>().join("\n"))?;
        Ok(())
    }

    fn finalize(mut self) -> Result<(), Box<dyn Error>> {
        self.issue_file.write_all(b"\n]")?;
        self.issue_file.flush()?;
        Ok(())
    }
}

fn export_issues_to_json(data_dir: &str, issues: &[Value]) -> Result<(), Box<dyn Error>> {
    let mut store = IssueStore::new(data_dir)?;
    store.add_issues(issues)?;
    Ok(())
}

fn process_wordlist(data_dir: &str, issues: &[Value]) -> Result<usize, Box<dyn Error>> {
    let mut words = HashSet::new();
    let re = Regex::new(r"\W+").unwrap();

    for issue in issues {
        let title = issue.get("title").and_then(|v| v.as_str()).unwrap_or_default();
        let description = issue.get("description").and_then(|v| v.as_str()).unwrap_or_default();

        let combined = format!("{} {}", title, description);
        for word in combined.split_whitespace() {
            let cleaned = re.replace_all(word, "").to_lowercase();
            if cleaned.len() >= 3 {
                words.insert(cleaned);
            }
        }
    }

    let word_count = words.len();
    let wordlist_path = Path::new(data_dir).join("wordlist.txt");
    fs::write(&wordlist_path, words.into_iter().collect::<Vec<_>>().join("\n"))?;
    println!("Wordlist updated: {} unique words", word_count);
    Ok(word_count)
}

async fn cleanup(data_dir: &str) -> Result<(), Box<dyn Error>> {
    println!("Performing cleanup...");
    // Save any pending state changes
    if let Ok(state) = ProgramState::load(data_dir) {
        state.save(data_dir)?;
    }
    println!("Cleanup complete.");
    Ok(())
}

async fn run_daemon(settings: Settings, running: Arc<AtomicBool>) -> Result<(), Box<dyn Error>> {
    println!("Starting GitLab Issue Cloner in daemon mode...");
    
    while running.load(Ordering::SeqCst) {
        let start_time = Instant::now();
        
        if let Err(e) = fetch_and_process_issues(&settings, &running).await {
            if !running.load(Ordering::SeqCst) {
                break; // Exit if shutdown was requested
            }

            if e.to_string().contains("rate limit") {
                println!("Rate limit reached. Waiting {} minutes before retry...", 
                    settings.rate_limit_reset_duration / 60);
                
                // Check shutdown signal while waiting
                let wait_until = Instant::now() + Duration::from_secs(settings.rate_limit_reset_duration);
                while Instant::now() < wait_until && running.load(Ordering::SeqCst) {
                    sleep(Duration::from_secs(1)).await;
                }
            } else {
                eprintln!("Error: {}. Retrying in 1 minute...", e);
                
                // Check shutdown signal while waiting
                let wait_until = Instant::now() + Duration::from_secs(60);
                while Instant::now() < wait_until && running.load(Ordering::SeqCst) {
                    sleep(Duration::from_secs(1)).await;
                }
            }
        } else {
            println!("Waiting {} minutes before next check...", settings.polling_interval / 60);
            
            // Check shutdown signal while waiting
            let wait_until = Instant::now() + Duration::from_secs(settings.polling_interval);
            while Instant::now() < wait_until && running.load(Ordering::SeqCst) {
                sleep(Duration::from_secs(1)).await;
            }
        }

        if !running.load(Ordering::SeqCst) {
            break;
        }
    }

    // Perform cleanup on shutdown
    cleanup(&settings.data_dir).await?;
    println!("Daemon shutdown complete.");
    Ok(())
}

async fn fetch_and_process_issues(settings: &Settings, running: &Arc<AtomicBool>) -> Result<(), GitlabError> {
    let client = Client::new();
    let state = ProgramState::load(&settings.data_dir)?;
    println!("Total issues processed so far: {}", state.total_issues);
    
    let (issues, updated_state) = fetch_issues(&client, &settings.default_project, settings, running).await?;
    if issues.is_empty() {
        println!("No new issues found");
        return Ok(());
    }
    
    println!("Found {} new issues to process", issues.len());
    export_issues_to_json(&settings.data_dir, &issues)?;
    process_wordlist(&settings.data_dir, &issues)?;
    
    // Save final state
    updated_state.save(&settings.data_dir)?;
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut settings = Settings::new().unwrap_or_else(|_| {
        println!("Failed to load settings. Using default values.");
        Settings {
            default_project: "gitlab-org/gitlab".to_string(),
            data_dir: "data".to_string(),
            initial_retry_delay: 1,
            max_retry_delay: 60,
            rate_limit_reset_duration: 90,
            polling_interval: 15, // Set to 30 minutes
            per_page: 1000,
            max_concurrent_requests: 10,
        }
    });

    // Ensure polling_interval is reasonable
    if settings.polling_interval < 300 { // minimum 5 minutes
        println!("Polling interval too low. Setting to minimum of 5 minutes.");
        settings.polling_interval = 300;
    }

    fs::create_dir_all(&settings.data_dir)?;

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        println!("\nReceived shutdown signal. Cleaning up...");
        r.store(false, Ordering::SeqCst);
    })?;

    if let Err(e) = run_daemon(settings.clone(), running).await {
        eprintln!("Error in daemon: {}", e);
        cleanup(&settings.data_dir).await?;
    }

    Ok(())
}
