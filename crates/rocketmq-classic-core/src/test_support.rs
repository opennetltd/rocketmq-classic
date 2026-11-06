use rocketmq_classic_common::message::{Message, MessageExt};

pub(crate) fn message_ext_from(message: Message) -> MessageExt {
    let mut extended = MessageExt::new();
    extended.set_topic_nullable(message.topic_nullable().map(str::to_owned));
    extended.set_body_nullable(message.body_nullable().map(|body| body.to_vec()));
    extended.set_flag(message.flag());
    let properties = message
        .properties_nullable()
        .is_some()
        .then(|| message.nullable_property_entries().collect());
    extended.set_nullable_properties(properties);
    extended.set_transaction_id(message.transaction_id().map(str::to_owned));
    extended
}
