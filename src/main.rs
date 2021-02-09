// Auto-formatted with `cargo fmt`
// `cargo run http://website.url/` to build+run in debug mode

#[macro_use]
extern crate fstrings; // for string interpolation to work: see *f! macros

use std::collections::BTreeSet; // you can also use HashSet, but they are slower in Rust by default (because of the cryptographically secure hash function)
use std::collections::VecDeque; // Queue type based on Vec (dynamic array)
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::prelude::*;
use std::path::PathBuf;
use std::ffi::OsStr;
use std::sync::Arc; // Atomic-reference-counted: wrapper for a long-live value that can be passed betwee threads
use std::sync::RwLock;
use std::{thread, time};

use futures::future::TryFutureExt; // Needed to enable unwrap_or_else mixin; in Rust just adding an "import" can unlock additional methods for objects!
use reqwest::header::CONTENT_TYPE;
use select::document::Document; // HTML parser
use select::predicate::Name;
use url::Url;

#[derive(Default)] // This means the default "constructor" will be auto-generated
struct CrawlerState {
    all_urls: BTreeSet<Arc<str>>, // set of shared, thread-safe strings
    to_fetch: VecDeque<Arc<str>>,
    in_progress: BTreeSet<Arc<str>>,
}

// Methods for the struct
impl CrawlerState {
    fn enqueue(&mut self, url: Arc<str>) {
        if !self.all_urls.contains(&url) {
            self.all_urls.insert(url.clone()); // explicit .clone() is needed when working with Arc or Rc
            self.to_fetch.push_back(url.clone());
        }
    }
}

struct CrawlerConfig {
    base_dir: PathBuf,
    base_url: Url,
}

#[derive(Clone)] // Generates .clone() method, which is needed to pass the object to another thread.
struct Crawler {
    config: Arc<CrawlerConfig>,
    state: Arc<RwLock<CrawlerState>>, // Shared mutable object, guarded by mutex for transactional safety
}

fn url_to_file_path(url: &Url) -> PathBuf {
    let mut res = PathBuf::new();
    let host = url.host_str().unwrap_or("unknown_host");
    res.push(host);
    if let Some(path_segs) = url.path_segments() {
        for s in path_segs {
            res.push(s);
        }
    }
    if url.path().ends_with("/") {
        res.push("index.html");
    }
    let mut fname = res.file_name().unwrap_or(OsStr::new("index.html")).to_owned();
    if let Some(q) = url.query() {
        fname.push("?");
        fname.push(q);
    }
    res.set_file_name(fname);
    res // last expression in the function is `return`ed
}


impl Crawler {


    // The Result type is Rust equivalient of Go's (result, error) pair, but you can only return
    // result (Ok) OR error (Err), not both.
    // `std::error::Error` is a trait (similar to interface), but in Go interface is always a pointer
    // resolved at runtime, while Rust tries to resolve traits at compile time. But this function
    // returns different kinds of errors, so we need to use Box<dyn> to return a pointer.
    // () is an empty type (void)
    async fn process_file(&self, url_str: &str) -> Result<(), Box<dyn std::error::Error>> {
        let url = Url::parse(url_str)?;
        let path = self.config.base_dir.join(url_to_file_path(&url));
        let path_s = path.to_str().unwrap();
        println_f!("Downloading {url} into {path_s}");
        fs::create_dir_all(path.parent().unwrap())?;
        // `await` actually runs the async function (future)
        // `?` is the equivalent of Go's `if err != nil { return err }`
        let res = reqwest::get(url.clone()).await?;
        let mime = res
            .headers()
            .get(CONTENT_TYPE)
            // the return value of .get() is Option<Header>. Option can be None.
            // `unwrap` is the equivalent of `if x = nil { panic("x is nil") }`
            // Note: `unwrap` also works on the Result<> type: `if err != nil { panic(err) }`
            .unwrap()
            .to_str()?;
        if mime.starts_with("text/html") {
            // Only save HTML files

            // "File" type is a file handle. Operations on file change internal values of the handle,
            // so we have to declare the variable as "mut". In Rust, you cannot easily share mutable
            // obects, you need to use RefCell or RwLock
            let mut file: File = File::create(&path)?;
            let text = res.text().await?;
            // If you don't use "unwrap" here, you get a warning about a potentially unhandled error
            file.write_all(text.as_bytes())?;
            let urls = extract_links(&text);

            // Note: this "for" loop can only be written once. If you attempt to write
            // `for link_url in urls` after this loop, it will fail. Why? Because, the `urls` object
            // of type Vec<String> owns its strings (they are destroyed when `urls` is destroyed).
            // By default, assignments in Rust transfer the object ownership.
            // So, this loop destroys the `urls` object and creates independent `link_url` strings.
            // If you want to keep `urls` object alive, you need to iterate over the borrowed
            // version:
            // for link_url in &urls
            // Then link_url will be of type &str, still dependent on `urls` object. If you need
            // to store these strings longer than `urls` exists, you would need to convert them
            // with .to_owned()
            for link_url in urls {
                let mut url2 = url.join(&link_url)?;
                url2.set_fragment(None);
                let url2s: Arc<str> = Arc::from(url2.into_string());
                if url2s.starts_with(self.config.base_url.as_str()) {
                    self.state.write().unwrap().enqueue(Arc::from(url2s));
                }
            }
        }
        return Ok(());  // The return type is Result<(),...>, not (), so we need to return something.
    }
}

// Extracts links from HTML file on disk.
fn extract_links(html: &str) -> Vec<String> {
    // Vec is a dynamic array type
    // The function consists of single expression - no need for `return`, no `;` at the end
    Document::from(html)
        .find(Name("a"))
        .filter_map(|node| node.attr("href")) // `|x| expr` is a lambda function (like `x -> expr` in Typescript)
        // `x` here has type &str, it's a pointer to string inside Node, which is a part of Document.
        // At the end of this function, the Document and its nodes will be destroyed.
        // We need the generated strings to live longer than that, so we are converting them to
        // "owned" (&str -> String)
        .map(|x| x.to_owned())
        .collect()
}

#[tokio::main] // Needed to enable async functions
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let base_url_str = args.get(1).expect("Expected argument: starting URL"); // "expect" means if x == nil { panic(message) }
    let base_url = Url::parse(base_url_str)?;
    let base_dir = args.get(2).map(|x| &**x).unwrap_or(".");
    let config = Arc::new(CrawlerConfig {
        base_url: base_url.clone(),
        base_dir: PathBuf::from(base_dir),
    });

    let state = Arc::new(RwLock::new(CrawlerState::default()));
    // State is a shared mutable object under a lock. To use it, we need to get a "transaction",
    // by using `.write().unwrap()` (here, .unwrap() will panic if the lock was poisoned by a panic
    // in another transaction).
    state
        .write()
        .unwrap()
        .enqueue(Arc::from(base_url.to_string()));
    // Since we do not store this "transaction" anywhere, it is immediately closed and the lock is
    // freed.

    let crawler = Crawler {
        config: config,
        state: state,
    };

    let max_concurrent = 20;

    loop {
        {
            // Here, we are storing the "transaction" in `s`.
            // To ensure `s` is destroyed and the "transaction" is closed, we need to put this
            // code into a separate { ... } block.
            let s = crawler.state.read().unwrap();
            if s.to_fetch.is_empty() && s.in_progress.is_empty() {
                break;
            }
        }
        while crawler.state.read().unwrap().in_progress.len() >= max_concurrent {
            thread::sleep(time::Duration::from_millis(100));
        }

        let maybe_url = crawler.state.write().unwrap().to_fetch.pop_front();
        let to_fetch = crawler.state.read().unwrap().to_fetch.len();
        let in_progress = crawler.state.read().unwrap().in_progress.len();
        println_f!("In progress: {in_progress}, in queue: {to_fetch}");
        match maybe_url {
            Some(url) => {
                crawler
                    .state
                    .write()
                    .unwrap()
                    .in_progress
                    .insert(url.clone());

                // We need to make copies of all the objects we are going to pass (potentially)
                // to another thread.
                let url_copy = url.clone();
                let crawler_copy = crawler.clone();
                // Note the `move` keyword. It demands that EVERY object referenced within
                // can be "moved" to a new ownership (i.e. into another thread) and will not be
                // used by the original code anymore. So, now, `crawler` and `crawler_copy` objects
                // are independent and they can be independenly destroyed by different threads,
                // but they refer to the same state via Arc.
                tokio::spawn(async move {
                    crawler_copy
                        .process_file(&url_copy)
                        .unwrap_or_else(|e| {
                            println!("ERROR fetching {:?} {:?}", &url_copy, e);
                        })
                        .await;
                    crawler_copy
                        .state
                        .write()
                        .unwrap()
                        .in_progress
                        .remove(&url_copy);
                });
            }
            None => {
                thread::sleep(time::Duration::from_millis(1000));
            }
        }
    }

    Ok(())
}
