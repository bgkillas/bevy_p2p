#![allow(clippy::shadow_reuse)]
use bevy_ecs::prelude::IntoSystem;
use bevy_ecs::system::{SystemInput, SystemParam};
#[derive(SystemParam)]
pub struct Runtime<'w, 's> {
    #[cfg(not(target_family = "wasm"))]
    pub runtime: bevy_ecs::system::Res<'w, bevy_tokio_tasks::TokioTasksRuntime>,
    #[cfg(target_family = "wasm")]
    pub commands: bevy_ecs::system::Commands<'w, 's>,
    #[cfg(not(target_family = "wasm"))]
    phantom: std::marker::PhantomData<&'s ()>,
}
#[cfg(not(target_family = "wasm"))]
impl Runtime<'_, '_> {
    pub fn spawn<I: SystemInput + Send + 'static, M: 'static>(
        &mut self,
        fun: impl IntoSystem<I, (), M> + Send + 'static,
        future: impl Future<Output = <I as SystemInput>::Inner<'static>> + Send + 'static,
    ) where
        for<'a> <I as SystemInput>::Inner<'a>: Send,
    {
        self.runtime
            .spawn_background_task(move |mut tasks| async move {
                let ret = future.await;
                tasks
                    .run_on_main_thread(move |main| {
                        main.world.run_system_cached_with(fun, ret).unwrap();
                    })
                    .await;
            });
    }
    pub fn spawn_loose(&mut self, future: impl Future<Output = ()> + Send + 'static) {
        self.runtime.runtime().spawn(future);
    }
}
#[cfg(target_family = "wasm")]
impl Runtime<'_, '_> {
    pub fn spawn<I: SystemInput + Send + 'static, M: 'static>(
        &mut self,
        fun: impl IntoSystem<I, (), M> + Send + 'static,
        future: impl Future<Output = <I as SystemInput>::Inner<'static>> + 'static,
    ) where
        for<'a> <I as SystemInput>::Inner<'a>: Send,
    {
        let (sender, receiver) = std::sync::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            sender.send(future.await).unwrap();
        });
        let ret = receiver.recv().unwrap();
        self.commands.run_system_cached_with(fun, ret);
    }
    pub fn spawn_loose(&mut self, future: impl Future<Output = ()> + 'static) {
        wasm_bindgen_futures::spawn_local(future);
    }
}
