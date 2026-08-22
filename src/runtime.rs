use bevy_ecs::prelude::IntoSystem;
use bevy_ecs::resource::Resource;
use bevy_ecs::system::{Commands, Res, SystemInput};
use std::num::NonZeroUsize;
use std::sync::mpmc;
use std::sync::mpmc::{Receiver, Sender};
use std::thread::available_parallelism;
#[derive(Resource)]
pub struct Runtime {
    #[cfg(not(target_family = "wasm"))]
    pub runtime: tokio::runtime::Runtime,
    pub tasks_send: Sender<Box<dyn FnOnce(&mut Commands) + Send + 'static>>,
    pub tasks: Receiver<Box<dyn FnOnce(&mut Commands) + Send + 'static>>,
}
impl Default for Runtime {
    fn default() -> Self {
        let (tasks_send, tasks) = mpmc::channel();
        let cpus = available_parallelism().map_or(1, NonZeroUsize::get);
        Self {
            #[cfg(not(target_family = "wasm"))]
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads((cpus - 2).max(1))
                .enable_all()
                .build()
                .unwrap(),
            tasks_send,
            tasks,
        }
    }
}
#[cfg(not(target_family = "wasm"))]
impl Runtime {
    pub fn spawn_hook<I: SystemInput + Send + 'static, M: 'static>(
        &self,
        fun: impl IntoSystem<I, (), M> + Send + 'static,
        future: impl Future<Output = <I as SystemInput>::Inner<'static>> + Send + 'static,
    ) where
        for<'a> <I as SystemInput>::Inner<'a>: Send,
    {
        let send = self.tasks_send.clone();
        self.spawn(async move {
            let ret = future.await;
            send.send(Box::new(|commands: &mut Commands| {
                commands.run_system_cached_with(fun, ret);
            }))
            .unwrap();
        });
    }
    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.runtime.spawn(future);
    }
    pub fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        self.runtime.block_on(future)
    }
}
#[cfg(target_family = "wasm")]
impl Runtime {
    pub fn spawn_hook<I: SystemInput + Send + 'static, M: 'static>(
        &self,
        fun: impl IntoSystem<I, (), M> + Send + 'static,
        future: impl Future<Output = <I as SystemInput>::Inner<'static>> + 'static,
    ) where
        for<'a> <I as SystemInput>::Inner<'a>: Send,
    {
        let send = self.tasks_send.clone();
        self.spawn(async move {
            let ret = future.await;
            send.send(Box::new(|commands: &mut Commands| {
                commands.run_system_cached_with(fun, ret);
            }))
            .unwrap();
        });
    }
    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        wasm_bindgen_futures::spawn_local(future);
    }
    pub fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        bevy_platform::future::block_on(future)
    }
}
pub fn run_tasks(mut commands: Commands, runtime: Res<Runtime>) {
    while let Ok(task) = runtime.tasks.try_recv() {
        task(&mut commands);
    }
}
