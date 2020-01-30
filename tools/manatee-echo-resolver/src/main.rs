//
// Copyright 2020 Joyent, Inc.
//

//
// Runs a manatee primary resolver and prints its output on stdout. Requires a
// running ZooKeeper instance to connect to.
//
// Manually edit the ZooKeeper node data and watch the changes come through!
//
// The -l argument is useful for controlling the amount of log spam. Set it to
// "critical" to see only the data that a client would.
//

use std::sync::mpsc::channel;
use std::thread;

use clap::{App, Arg};
use cueball::resolver::{BackendMsg, Resolver};
use slog::Level;

use common::util;
use cueball_manatee_primary_resolver::{
    common, ManateePrimaryResolver, ZkConnectString,
};

const DEFAULT_CONN_STR: &str = "127.0.0.1:2181";
const DEFAULT_PATH: &str = "/manatee/1.boray.virtual.example.com";

fn main() {
    let matches = App::new("Manatee Primary Resolver CLI")
        .version("0.1.0")
        .author("Isaac Davis <isaac.davis@joyent.com>")
        .about("Echoes resolver output to command line")
        .arg(
            Arg::with_name("connect string")
                .short("c")
                .long("connect-string")
                .takes_value(true)
                .help(
                    format!(
                        "Comma-separated list of ZooKeeper address:port \
                         pairs\n\
                         (default: {})",
                        DEFAULT_CONN_STR
                    )
                    .as_str(),
                ),
        )
        .arg(
            Arg::with_name("root path")
                .short("p")
                .long("root-path")
                .takes_value(true)
                .help(
                    format!(
                        "Root path of manatee cluster in ZooKeeper \
                         (NOT including '/state' node)\n\
                         (default: {:?})",
                        DEFAULT_PATH
                    )
                    .as_str(),
                ),
        )
        .arg(
            Arg::with_name("log level")
                .short("l")
                .long("log-level")
                .takes_value(true)
                .help(
                    format!(
                        "Log level: trace|debug|info|warning|error|critical\n\
                         (uses {} env var if flag not given)\n\
                         (default: {})",
                        util::LOG_LEVEL_ENV_VAR,
                        format!("{:?}", util::DEFAULT_LOG_LEVEL).to_lowercase()
                    )
                    .as_str(),
                ),
        )
        .get_matches();

    let s = matches
        .value_of("connect string")
        .unwrap_or(DEFAULT_CONN_STR)
        .parse::<ZkConnectString>()
        .expect("Invalid ZK connect string");
    let p = matches
        .value_of("root path")
        .unwrap_or(DEFAULT_PATH)
        .to_string();

    //
    // Try to get the log level from the CLI arg and, if that fails, the
    // environment variable. If both fail, just use the default.
    //
    let l = match matches.value_of("log level") {
        Some(level_str) => util::parse_log_level(level_str.to_string())
            .expect("Invalid log level"),
        None => util::log_level_from_env()
            .expect("Invalid log level")
            .unwrap_or(util::DEFAULT_LOG_LEVEL),
    };

    std::process::exit(match run(s, p, l) {
        Ok(_) => 0,
        Err(err) => {
            eprintln!("error: {:?}", err);
            1
        }
    });
}

fn run(s: ZkConnectString, p: String, l: Level) -> Result<(), String> {
    let log = util::standard_log(l);

    let (tx, rx) = channel();

    thread::spawn(move || {
        let mut resolver = ManateePrimaryResolver::new(s, p, Some(log));
        resolver.run(tx);
    });

    loop {
        match rx.recv() {
            Ok(msg) => match msg {
                BackendMsg::AddedMsg(msg) => {
                    println!("Added msg: {:?}", msg.key)
                }
                BackendMsg::RemovedMsg(msg) => {
                    println!("Removed msg: {:?}", msg.0)
                }
                BackendMsg::StopMsg => {
                    println!("Stop msg");
                    break;
                }
                BackendMsg::HeartbeatMsg => println!("Heartbeat msg"),
            },
            Err(e) => {
                return Err(format!("Error receiving on channel: {:?}", e));
            }
        }
    }

    Ok(())
}
