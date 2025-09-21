CREATE TABLE TB_WORKER_SLOT (
                                WORKER_ID      NUMBER(10,0)   NOT NULL,             -- PK: 0..N, 사전 채움
                                INSTANCE_TOKEN VARCHAR2(100 CHAR),                  -- 현재 소유자 식별(없으면 미임차)
                                LEASE_UNTIL    TIMESTAMP(6) WITH LOCAL TIME ZONE,                        -- 임차 만료(가시성 타임아웃)
                                HEARTBEAT_AT   TIMESTAMP(6) WITH LOCAL TIME ZONE,                        -- 마지막 하트비트

                                CREATED_AT     TIMESTAMP(6) WITH LOCAL TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                                UPDATED_AT     TIMESTAMP(6) WITH LOCAL TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,

                                CONSTRAINT PK_WORKER_SLOT PRIMARY KEY (WORKER_ID)
);

-- 한 토큰이 동시에 여러 슬롯을 점유하지 못하도록 (NULL 다수 허용)
CREATE UNIQUE INDEX UQ_WORKER_SLOT_TOKEN ON TB_WORKER_SLOT (INSTANCE_TOKEN);

-- 스캐닝/회수 최적화
CREATE INDEX IX_WORKER_SLOT_LEASE ON TB_WORKER_SLOT (LEASE_UNTIL);
