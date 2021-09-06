CREATE SCHEMA IF NOT EXISTS vrt;

CREATE TYPE vrt.blocking AS ENUM ('unblocked', 'blocked');
CREATE TYPE vrt.suspension AS ENUM ('active', 'suspended');

CREATE TABLE vrt.party
(
    id                         BIGSERIAL                   NOT NULL,
    event_id                   BIGINT                      NOT NULL,
    event_time                 TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    party_id                   CHARACTER VARYING           NOT NULL,
    created_at                 TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    email                      CHARACTER VARYING,
    blocking                   vrt.blocking                NOT NULL,
    blocked_reason             CHARACTER VARYING,
    blocked_since              TIMESTAMP WITHOUT TIME ZONE,
    unblocked_reason           CHARACTER VARYING,
    unblocked_since            TIMESTAMP WITHOUT TIME ZONE,
    suspension                 vrt.suspension              NOT NULL,
    suspension_active_since    TIMESTAMP WITHOUT TIME ZONE,
    suspension_suspended_since TIMESTAMP WITHOUT TIME ZONE,
    revision_id                CHARACTER VARYING,
    revision_changed_at        TIMESTAMP WITHOUT TIME ZONE
);

CREATE UNIQUE INDEX party_party_id_uidx ON vrt.party (party_id);
CREATE INDEX party_created_at_idx ON vrt.party (created_at);

CREATE TYPE vrt.contractor_type AS ENUM ('registered_user', 'legal_entity', 'private_entity');
CREATE TYPE vrt.legal_entity AS ENUM ('russian_legal_entity', 'international_legal_entity');
CREATE TYPE vrt.private_entity AS ENUM ('russian_private_entity');
CREATE TYPE vrt.contractor_identification_lvl AS ENUM ('none', 'partial', 'full');

CREATE TABLE vrt.shop
(
    id                                            BIGSERIAL                   NOT NULL,
    event_id                                      BIGINT                      NOT NULL,
    event_time                                    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    party_id                                      CHARACTER VARYING           NOT NULL,
    shop_id                                       CHARACTER VARYING           NOT NULL,

    contract_id                                   CHARACTER VARYING           NOT NULL,
    category_id                                   INT,
    payout_tool_id                                CHARACTER VARYING,
    payout_schedule_id                            INT,
    created_at                                    TIMESTAMP WITHOUT TIME ZONE,
    blocking                                      vrt.blocking,
    blocked_reason                                CHARACTER VARYING,
    blocked_since                                 TIMESTAMP WITHOUT TIME ZONE,
    unblocked_reason                              CHARACTER VARYING,
    unblocked_since                               TIMESTAMP WITHOUT TIME ZONE,
    suspension                                    vrt.suspension,
    suspension_active_since                       TIMESTAMP WITHOUT TIME ZONE,
    suspension_suspended_since                    TIMESTAMP WITHOUT TIME ZONE,
    details_name                                  CHARACTER VARYING,
    details_description                           CHARACTER VARYING,
    location_url                                  CHARACTER VARYING,
    account_currency_code                         CHARACTER VARYING,
    account_settlement                            CHARACTER VARYING,
    account_guarantee                             CHARACTER VARYING,
    account_payout                                CHARACTER VARYING,

    contractor_id                                 CHARACTER VARYING,
    contractor_type                               vrt.contractor_type,
    reg_user_email                                CHARACTER VARYING,
    legal_entity_type                             vrt.legal_entity,
    russian_legal_entity_name                     CHARACTER VARYING,
    russian_legal_entity_registered_number        CHARACTER VARYING,
    russian_legal_entity_inn                      CHARACTER VARYING,
    russian_legal_entity_actual_address           CHARACTER VARYING,
    russian_legal_entity_post_address             CHARACTER VARYING,
    russian_legal_entity_representative_position  CHARACTER VARYING,
    russian_legal_entity_representative_full_name CHARACTER VARYING,
    russian_legal_entity_representative_document  CHARACTER VARYING,
    russian_legal_entity_bank_account             CHARACTER VARYING,
    russian_legal_entity_bank_name                CHARACTER VARYING,
    russian_legal_entity_bank_post_account        CHARACTER VARYING,
    russian_legal_entity_bank_bik                 CHARACTER VARYING,

    international_legal_entity_name               CHARACTER VARYING,
    international_legal_entity_trading_name       CHARACTER VARYING,
    international_legal_entity_registered_address CHARACTER VARYING,
    international_legal_entity_registered_number  CHARACTER VARYING,
    international_actual_address                  CHARACTER VARYING,
    international_legal_entity_country_code       CHARACTER VARYING,

    private_entity_type                           vrt.private_entity,
    russian_private_entity_first_name             CHARACTER VARYING,
    russian_private_entity_second_name            CHARACTER VARYING,
    russian_private_entity_middle_name            CHARACTER VARYING,
    russian_private_entity_phone_number           CHARACTER VARYING,
    russian_private_entity_email                  CHARACTER VARYING,

    contractor_identification_level               vrt.contractor_identification_lvl,
    CONSTRAINT shop_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX party_id_shop_id_uidx ON vrt.shop (party_id, shop_id);
CREATE INDEX shop_created_at_idx ON vrt.shop (created_at);
CREATE INDEX shop_party_id_contract_id_idx ON vrt.shop (party_id, contract_id);

CREATE TYPE vrt.contract_status AS ENUM ('active', 'terminated', 'expired');

CREATE TABLE vrt.contract
(
    id                          BIGSERIAL                   NOT NULL,
    event_id                    BIGINT                      NOT NULL,
    event_time                  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    party_id                    CHARACTER VARYING           NOT NULL,
    contract_id                 CHARACTER VARYING           NOT NULL,
    contractor_id               CHARACTER VARYING           NOT NULL,
    created_at                  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    valid_since                 TIMESTAMP WITHOUT TIME ZONE,
    valid_until                 TIMESTAMP WITHOUT TIME ZONE,
    status                      vrt.contract_status,
    status_terminated_at        TIMESTAMP WITHOUT TIME ZONE,
    legal_agreement_signed_at   TIMESTAMP WITHOUT TIME ZONE,
    legal_agreement_id          CHARACTER VARYING,
    legal_agreement_valid_until TIMESTAMP WITHOUT TIME ZONE,

    CONSTRAINT contract_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX party_id_contract_id_uidx ON vrt.contract (party_id, contract_id);

CREATE TABLE vrt.contractor
(
    id                                            BIGSERIAL                   NOT NULL,
    event_id                                      BIGINT                      NOT NULL,
    event_time                                    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    party_id                                      CHARACTER VARYING           NOT NULL,

    contractor_id                                 CHARACTER VARYING           NOT NULL,
    contractor_type                               vrt.contractor_type,
    reg_user_email                                CHARACTER VARYING,
    legal_entity_type                             vrt.legal_entity,
    russian_legal_entity_name                     CHARACTER VARYING,
    russian_legal_entity_registered_number        CHARACTER VARYING,
    russian_legal_entity_inn                      CHARACTER VARYING,
    russian_legal_entity_actual_address           CHARACTER VARYING,
    russian_legal_entity_post_address             CHARACTER VARYING,
    russian_legal_entity_representative_position  CHARACTER VARYING,
    russian_legal_entity_representative_full_name CHARACTER VARYING,
    russian_legal_entity_representative_document  CHARACTER VARYING,
    russian_legal_entity_bank_account             CHARACTER VARYING,
    russian_legal_entity_bank_name                CHARACTER VARYING,
    russian_legal_entity_bank_post_account        CHARACTER VARYING,
    russian_legal_entity_bank_bik                 CHARACTER VARYING,

    international_legal_entity_name               CHARACTER VARYING,
    international_legal_entity_trading_name       CHARACTER VARYING,
    international_legal_entity_registered_address CHARACTER VARYING,
    international_legal_entity_registered_number  CHARACTER VARYING,
    international_actual_address                  CHARACTER VARYING,
    international_legal_entity_country_code       CHARACTER VARYING,

    private_entity_type                           vrt.private_entity,
    russian_private_entity_first_name             CHARACTER VARYING,
    russian_private_entity_second_name            CHARACTER VARYING,
    russian_private_entity_middle_name            CHARACTER VARYING,
    russian_private_entity_phone_number           CHARACTER VARYING,
    russian_private_entity_email                  CHARACTER VARYING,

    contractor_identification_level               vrt.contractor_identification_lvl,

    CONSTRAINT contractor_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX party_id_contractor_id_idx ON vrt.contractor (party_id, contractor_id);

CREATE TABLE vrt.category
(
    id          BIGSERIAL         NOT NULL,
    version_id  BIGINT            NOT NULL,
    category_id INT               NOT NULL,
    name        CHARACTER VARYING NOT NULL,
    description CHARACTER VARYING NOT NULL,
    type        CHARACTER VARYING,
    deleted     BOOLEAN DEFAULT false,
    CONSTRAINT category_pkey PRIMARY KEY (id)
);

CREATE INDEX category_version_id on vrt.category (version_id);
CREATE INDEX category_idx on vrt.category (category_id);

CREATE TABLE vrt.dominant
(
    last_version BIGINT NOT NULL,
    CONSTRAINT dominant_pkey PRIMARY KEY (last_version)
)

