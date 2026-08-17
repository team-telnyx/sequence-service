-- SV2-044 r3: versioned reply-intent classification on signals.
-- Adds the ReplyIntent enum type and a nullable reply_intent column on signals.
-- The enum values MUST match src/contracts.py ReplyIntent and the v2 side
-- packages/contracts/sequence.py ReplyIntent EXACTLY (enforced by
-- tests/contract/test_sequence_contract_compat.py on the v2 side and
-- tests/test_sv2_044_contract.py on the server side).
--
-- reply_intent is NULL for non-reply signals (BOUNCE/OPEN/CLICK/UNSUBSCRIBE)
-- and set ONLY for signals that carry a reply intent (REPLY -> UNKNOWN pending
-- deeper content analysis; OUT_OF_OFFICE -> OUT_OF_OFFICE). Bounce is
-- structurally distinct from REPLY (v1's poller conflated them; do not recur).

CREATE TYPE reply_intent AS ENUM (
    'positive_interest',
    'positive_meeting',
    'negative_not_interested',
    'negative_wrong_person',
    'out_of_office',
    'autoresponder',
    'unsubscribe_request',
    'unknown'
);

ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS reply_intent reply_intent;
