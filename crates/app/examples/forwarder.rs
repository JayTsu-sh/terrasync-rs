// 示例代码：演示 BroadcastForwarder 用法，sample 性质豁免 unwrap deny
#![allow(clippy::unwrap_used)]

use std::time::SystemTime;

use app::broadcast::BroadcastForwarder;

#[tokio::main]
async fn main() {
    let mut forwarder = BroadcastForwarder::new(1000);

    // 添加两个消费者
    let mut rx1 = forwarder.subscribe();
    let mut rx2 = forwarder.subscribe();

    // 消费者任务
    let consumer1 = tokio::spawn(async move {
        while let Some(msg) = rx1.recv().await {
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            println!("[{:?}] 消费者1收到: {}", timestamp, msg);
            // 模拟慢处理
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        println!("[{:?}] 消费者1处理完毕", timestamp);
    });
    let consumer2 = tokio::spawn(async move {
        while let Some(msg) = rx2.recv().await {
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            println!("[{:?}] 消费者2收到: {}", timestamp, msg);
        }
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        println!("[{:?}] 消费者2处理完毕", timestamp);
    });

    // 生产者发送消息
    for i in 0..10000 {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        println!("[{:?}] 发送消息 {}", timestamp, i);
        forwarder.broadcast(i).await;
    }

    // 等待所有消费者任务完成
    consumer1.await.unwrap();
    consumer2.await.unwrap();

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    println!("[{:?}] 所有消息处理完毕，程序退出", timestamp);
}
