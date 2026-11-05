# Versioned formal profile migration

`tools.build_formal_profile_adapter` renders immutable adapter source from the exact approved adapter and confirmed batch snapshot. Rendering does not prepare a task, run permission probes, register policy, start a consumer or call a model.

The optional `probe_scope="task_local_v1"` renders the task-local public/private target contract. It checks the frozen public tree, frozen answer/rubric, the task’s own neutral marker and teacher symlink. Additional read-probe paths are rejected. Existing ordinary tool, write and local IPC gates remain. This contract does not prove credential, parent-process or global-session isolation.

`profile-migration.json` and the source-bound solver profile packet must carry the same explicit `probe_scope`. The profile guard reconstructs all five adapter files and rejects mismatched scope or generated source drift. Omitting the option preserves the original renderer output byte for byte. Current host evidence is supplied separately through the existing `runtime_support_refresh` contract; historical observations remain unchanged.

After code installation, verify the exact candidate profile through the ordinary profile guard before registering it. Independent business quality and the normal formal preparation gates remain mandatory; generating a profile never authorizes formal dispatch.
