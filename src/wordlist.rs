use serde_json::Value;
use std::collections::HashSet;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};

pub fn update_wordlist(issues: &[Value]) -> Result<usize, Box<dyn Error>> {
    let mut words = load_existing_words()?;
    let initial_count = words.len();
    
    for (i, issue) in issues.iter().enumerate() {
        if (i + 1) % 100 == 0 {
            println!("Processing issue {}/{}", i + 1, issues.len());
        }
        
        let title = issue["title"].as_str().unwrap_or_default();
        let desc = issue["description"].as_str().unwrap_or_default();
        
        words.extend(extract_words(title));
        words.extend(extract_words(desc));
    }

    let new_words = words.len() - initial_count;
    if new_words > 0 {
        println!("Added {} new unique words", new_words);
    }

    save_words(&words)?;
    Ok(words.len())
}

fn load_existing_words() -> Result<HashSet<String>, Box<dyn Error>> {
    let file = match File::open("wordlist.txt") {
        Ok(f) => f,
        Err(_) => return Ok(HashSet::new()),
    };
    
    let reader = BufReader::new(file);
    Ok(reader.lines().filter_map(Result::ok).collect())
}

fn extract_words(text: &str) -> HashSet<String> {
    text.split_whitespace()
        .map(|w| w.trim_matches(|c: char| !c.is_alphabetic()).to_lowercase())
        .filter(|w| !w.is_empty() && w.len() > 2)
        .collect()
}

fn save_words(words: &HashSet<String>) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open("wordlist.txt")?;

    for word in words {
        writeln!(file, "{}", word)?;
    }
    Ok(())
}