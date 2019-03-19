extern crate clap;
pub mod server;
use clap::{App, SubCommand};
use server::paustdb::serve;

const RUN_COMMAND: &'static str = "run";

fn main() {

    let matches = App::new("PaustDB")
                                  .about("Decentralized TSDB specialized for real-time streaming")
                                  .version("0.0.1")
                                  .author("Andrew joo")
                                  .subcommand(run_commands_definition())
        .get_matches();

    match matches.subcommand_name() {
        Some(RUN_COMMAND) => serve(),
        None => println!("none"),
        _ => unreachable!(),
    }
}

fn run_commands_definition<'a, 'b>() -> App<'a, 'b> {
    SubCommand::with_name(RUN_COMMAND)
        .about("Run paustdb server")
}
