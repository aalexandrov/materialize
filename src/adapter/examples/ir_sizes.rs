use mz_repr::explain::trace_plan;
use mz_repr::explain::tracing::PlanTrace;
use tracing::{self, span, subscriber, Dispatch};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{field, fmt};

#[tokio::main]
async fn main() {
    let layer_fmt_ansi = Box::new(fmt::layer().with_writer(std::io::stderr).with_ansi(true));
    let registry = tracing_subscriber::registry().with(layer_fmt_ansi);
    registry.init();

    tracing::info!("Hello, world!");

    let registry = DelegateSubscriber::default().with(PlanTrace::<String>::new(None));
    let registry = Dispatch::from(registry);

    use tracing::instrument::WithSubscriber;
    do_work().with_subscriber(registry.clone()).await;

    if let Some(trace) = registry.downcast_ref::<PlanTrace<String>>() {
        for (i, entry) in trace.drain_as_vec().iter().enumerate() {
            tracing::info!("results {i}: {:?}", entry.plan);
        }
    }

    tracing::info!("Good bye, world!");
}

#[tracing::instrument]
async fn do_work() {
    tracing::info!("Going to work!");

    do_task_1().await;
    do_task_2().await;

    trace_plan(&"do_work end".to_string());

    tracing::info!("Finished working!");
}

#[tracing::instrument]
async fn do_task_1() {
    tracing::info!("Doing task #1!");
    tracing::info!("Finished task #1!");

    trace_plan(&"task #1 end".to_string());
}

#[tracing::instrument(fields(path.segment = "task-2"))]
async fn do_task_2() {
    tracing::info!("Doing task #2!");
    tracing::info!("Finished task #2!");

    trace_plan(&"task #2 end".to_string());
}

// pub fn trace_msg(msg: &str) {
//     let span = tracing::Span::current();
//     span.with_subscriber(|(_id, subscriber)| {
//         if let Some(trace) = subscriber.downcast_ref::<DelegateSubscriber>() {
//             let mut plans = trace.plans.lock().expect("xxx");
//             plans.push(msg.to_owned());
//         }
//     });
// }

pub struct DelegateSubscriber {
    inner: Dispatch,
}

impl Default for DelegateSubscriber {
    fn default() -> Self {
        Self {
            inner: tracing::dispatcher::get_default(Clone::clone),
        }
    }
}

impl subscriber::Subscriber for DelegateSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        self.inner.enabled(metadata)
    }

    fn new_span(&self, span: &span::Attributes<'_>) -> span::Id {
        self.inner.new_span(span)
    }

    fn record(&self, span: &span::Id, values: &span::Record<'_>) {
        self.inner.record(span, values)
    }

    fn record_follows_from(&self, span: &span::Id, follows: &span::Id) {
        self.inner.record_follows_from(span, follows)
    }

    fn event(&self, event: &tracing::Event<'_>) {
        self.inner.event(event)
    }

    fn enter(&self, span: &span::Id) {
        self.inner.enter(span)
    }

    fn exit(&self, span: &span::Id) {
        self.inner.exit(span)
    }

    fn current_span(&self) -> tracing_core::span::Current {
        self.inner.current_span()
    }
}

/// Helper trait used to extract attributes of type `&'static str`.
trait GetStr {
    fn get_str(&self, key: &'static str) -> Option<String>;
}

impl<'a> GetStr for span::Attributes<'a> {
    fn get_str(&self, key: &'static str) -> Option<String> {
        let mut extract_str = ExtractStr::new(key);
        self.record(&mut extract_str);
        extract_str.val()
    }
}

/// Helper struct that implements `field::Visit` and is used in the
/// `GetStr::get_str` implementation for `span::Attributes`.
struct ExtractStr {
    key: &'static str,
    val: Option<String>,
}

impl ExtractStr {
    fn new(key: &'static str) -> Self {
        Self { key, val: None }
    }

    fn val(self) -> Option<String> {
        self.val
    }
}

impl field::Visit for ExtractStr {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == self.key {
            self.val = Some(value.to_string())
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}
