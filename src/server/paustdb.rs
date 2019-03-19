extern crate abci;

//use abci::*;

struct PaustApp {
    hash: u64,
}

impl PaustApp {
    fn new() -> PaustApp {
        PaustApp { hash : 0}
    }
}

impl abci::Application for PaustApp {
//    fn info() {
//
//    }
//
//    fn query() {
//
//    }
//
//    fn check_tx() {
//
//    }
//
//    fn init_chain() {
//
//    }
//
//    fn begin_block() {
//
//    }
//
//    fn deliver_tx() {
//
//    }
//
//    fn end_block() {dasfasfasfdasdasz
//
//    }
//
//    fn commit() {
//
//    }
}

pub fn serve() {
    let addr = "0.0.0.0:26658".parse().unwrap();
    abci::run(addr, PaustApp::new());
}
