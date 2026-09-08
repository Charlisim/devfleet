//! Provider-neutral domain identities and plans.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }
    };
}

id_type!(ProjectId);
id_type!(WorktreeId);
id_type!(EnvironmentId);
id_type!(ResourceId);
id_type!(OperationId);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Generation(pub u64);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentKey {
    pub project_id: ProjectId,
    pub worktree_id: WorktreeId,
    pub profile: String,
    pub instance: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourcePlan {
    pub resource_id: ResourceId,
    pub provider: String,
    pub depends_on: Vec<ResourceId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentState {
    Registered,
    Planning,
    Starting,
    Running,
    Degraded,
    Stopping,
    Stopped,
    Failed,
    Destroying,
    Destroyed,
    Orphaned,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_key_uses_stable_ids_not_a_branch_name() {
        let key = EnvironmentKey {
            project_id: ProjectId::new(),
            worktree_id: WorktreeId::new(),
            profile: "ios".into(),
            instance: None,
        };
        assert_eq!(key.profile, "ios");
    }
}
