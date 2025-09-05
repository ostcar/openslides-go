-- This script can only be used for an empty database without used sequences.
INSERT INTO
theme_t (id, name)
VALUES
(1, 'standard theme');

INSERT INTO
organization_t (name, theme_id)
VALUES
('Intevation', 1);

INSERT INTO
committee_t (id, name)
VALUES
(1, 'committee');

BEGIN;
INSERT INTO
meeting_t (
    id,
    default_group_id,
    admin_group_id,
    motions_default_workflow_id,
    motions_default_amendment_workflow_id,
    committee_id,
    reference_projector_id,
    name
)
VALUES
(1, 1, 2, 1, 1, 1, 1, 'meeting');

INSERT INTO
group_t (id, name, meeting_id, permissions)
VALUES
(
    1,
    'Default',
    1,
    '{
    "agenda_item.can_see",
    "assignment.can_see",
    "meeting.can_see_autopilot",
    "meeting.can_see_frontpage",
    "motion.can_see",
    "projector.can_see"
}'
),
(2, 'Admin', 1, DEFAULT);

INSERT INTO
motion_workflow_t (
    id,
    name,
    sequential_number,
    first_state_id,
    meeting_id
)
VALUES
(1, 'Simple Workflow', 1, 1, 1);

INSERT INTO
motion_state_t (
    id,
    name,
    weight,
    workflow_id,
    meeting_id,
    allow_create_poll,
    allow_support,
    set_workflow_timestamp,
    recommendation_label,
    css_class,
    merge_amendment_into_final
)
VALUES
(
    1,
    'submitted',
    1,
    1,
    1,
    true,
    true,
    true,
    'Submitted',
    'grey',
    'do_not_merge'
),
(
    2,
    'accepted',
    2,
    1,
    1,
    DEFAULT,
    DEFAULT,
    DEFAULT,
    'Acceptance',
    'green',
    'do_merge'
),
(
    3,
    'rejected',
    3,
    1,
    1,
    DEFAULT,
    DEFAULT,
    DEFAULT,
    'Rejection',
    'red',
    'do_not_merge'
),
(
    4,
    'not decided',
    4,
    1,
    1,
    DEFAULT,
    DEFAULT,
    DEFAULT,
    'No decision',
    'grey',
    'do_not_merge'
);

INSERT INTO projector_t (id, sequential_number, meeting_id)
VALUES (1, 1, 1);

COMMIT;
