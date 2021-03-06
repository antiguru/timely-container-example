extern crate timely;

use differential_dataflow::collection::Collection;
use differential_dataflow::{AsCollection, Hashable};
use rand::{Rng, SeedableRng};
use std::time::Instant;

use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Probe;
use timely::dataflow::{InputHandleCore, ProbeHandle};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input = InputHandleCore::new();
        let mut probe = ProbeHandle::new();

        // define a distribution function for strings.
        let exchange =
            Exchange::new(move |update: &((String, ()), usize, i32)| (update.0).0.hashed().into());

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>(|scope| {
            let collection: Collection<_, _, _> = input.to_stream(scope).as_collection();
            collection
                .arrange_core::<_, OrdValSpine<_, _, _, _>>(exchange, "word count")
                .stream
                .probe_with(&mut probe);
        });

        let mut rng = rand::rngs::SmallRng::from_seed([worker.index() as u8; 32]);

        println!("Constructing data");
        let batch_count = (1 << 14) / worker.peers();
        let size = 1024;
        let mut batches = Vec::new();
        for _ in 0..batch_count {
            print!(".");
            let mut stack = Vec::default();
            for _ in 0..size {
                let len = rng.gen_range(1..33);
                stack.push((
                    (
                        String::from_iter((0..len).map(|_| rng.gen_range('a'..'z'))),
                        (),
                    ),
                    1,
                    1,
                ));
            }
            batches.push(stack);
        }
        println!("Done");

        // introduce data and watch!
        let start = Instant::now();
        for (round, mut batch) in batches.into_iter().enumerate() {
            input.send_batch(&mut batch);
            input.advance_to((round / 4) + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
        let duration = start.elapsed();
        println!("Took {:?}", duration);
    })
    .unwrap();
}
