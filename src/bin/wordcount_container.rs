extern crate timely;

use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::time::Instant;
use timely::container::columnation::TimelyStack;

use timely::dataflow::channels::pact::ExchangeCore;
use timely::dataflow::operators::{Operator, Probe};
use timely::dataflow::{InputHandleCore, ProbeHandle};
use timely_communication::message::RefOrMut;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input = InputHandleCore::new();
        let mut probe = ProbeHandle::new();

        // define a distribution function for strings.
        let exchange = ExchangeCore::new(|x: &String| x.len() as u64);

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>(|scope| {
            input
                .to_stream(scope)
                .unary_frontier(exchange, "WordCount", |_capability, _info| {
                    let mut queues = HashMap::new();
                    let mut counts = HashMap::new();

                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            queues
                                .entry(time.retain())
                                .or_insert(Vec::new())
                                .push(RefOrMut::<TimelyStack<String>>::replace(
                                    data,
                                    Default::default(),
                                ));
                        }

                        for (key, val) in queues.iter_mut() {
                            if !input.frontier().less_equal(key.time()) {
                                let mut session = output.session(key);
                                for batch in val.drain(..) {
                                    for word in &batch[..] {
                                        let entry = counts.entry(word.clone()).or_insert(0i64);
                                        *entry += 1;
                                        session.give((word.clone(), *entry));
                                    }
                                }
                            }
                        }

                        queues.retain(|_key, val| !val.is_empty());
                    }
                })
                .probe_with(&mut probe);
        });

        let mut rng = rand::rngs::SmallRng::from_seed([worker.index() as u8; 32]);

        println!("Constructing data");
        let batch_count = (1 << 14) / worker.peers();
        let size = 1024;
        let mut batches = Vec::new();
        for _ in 0..batch_count {
            print!(".");
            let mut stack = TimelyStack::default();
            for _ in 0..size {
                let len = rng.gen_range(1..33);
                stack.copy(&String::from_iter(
                    (0..len).map(|_| rng.gen_range('a'..'z')),
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
