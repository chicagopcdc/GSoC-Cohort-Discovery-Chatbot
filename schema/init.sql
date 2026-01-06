-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. User Table
CREATE TABLE IF NOT EXISTS "User" (
  "id" UUID PRIMARY KEY DEFAULT public.uuid_generate_v4(),
  "identifier" TEXT NOT NULL UNIQUE,
  "metadata" JSONB DEFAULT '{}'::jsonb,
  "createdAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  "deletedAt" TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_user_identifier ON "User" ("identifier");

-- 2. Thread Table
CREATE TABLE IF NOT EXISTS "Thread" (
  "id" UUID PRIMARY KEY DEFAULT public.uuid_generate_v4(),
  "createdAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  "name" TEXT,
  "userId" UUID REFERENCES "User" ("id") ON DELETE CASCADE,
  "userIdentifier" TEXT,
  "tags" TEXT[],
  "metadata" JSONB DEFAULT '{}'::jsonb,
  "updatedAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  "deletedAt" TIMESTAMPTZ,
  "participant" JSONB
);
CREATE INDEX IF NOT EXISTS idx_thread_userid ON "Thread" ("userId");
CREATE INDEX IF NOT EXISTS idx_thread_useridentifier ON "Thread" ("userIdentifier");
CREATE INDEX IF NOT EXISTS idx_thread_createdat ON "Thread" ("createdAt" DESC);

-- 3. Element Table
CREATE TABLE IF NOT EXISTS "Element" (
  "id" UUID PRIMARY KEY DEFAULT public.uuid_generate_v4(),
  "threadId" UUID REFERENCES "Thread" ("id") ON DELETE CASCADE,
  "type" TEXT,
  "url" TEXT,
  "chainlitKey" TEXT,
  "name" TEXT NOT NULL,
  "display" TEXT,
  "objectKey" TEXT,
  "size" TEXT,
  "page" INTEGER,
  "forIds" TEXT[],
  "mime" TEXT,
  "updatedAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  "deletedAt" TIMESTAMPTZ,
  "createdAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_element_threadid ON "Element" ("threadId");

-- 4. Feedback Table
CREATE TABLE IF NOT EXISTS "Feedback" (
  "id" UUID PRIMARY KEY DEFAULT public.uuid_generate_v4(),
  "forId" UUID NOT NULL,
  "threadId" UUID NOT NULL REFERENCES "Thread" ("id") ON DELETE CASCADE,
  "value" INTEGER NOT NULL,
  "comment" TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  "deletedAt" TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_feedback_threadid ON "Feedback" ("threadId");
CREATE INDEX IF NOT EXISTS idx_feedback_forid ON "Feedback" ("forId");

-- 5. Step Table
CREATE TABLE IF NOT EXISTS "Step" (
  "id" UUID PRIMARY KEY DEFAULT public.uuid_generate_v4(),
  "name" TEXT DEFAULT 'step',
  "type" TEXT NOT NULL,
  "threadId" UUID REFERENCES "Thread" ("id") ON DELETE CASCADE,
  "parentId" UUID REFERENCES "Step" ("id") ON DELETE CASCADE,
  "streaming" BOOLEAN DEFAULT false,
  "waitForAnswer" BOOLEAN DEFAULT false,
  "isError" BOOLEAN DEFAULT false,
  "metadata" JSONB DEFAULT '{}'::jsonb,
  "tags" TEXT[],
  "input" TEXT,
  "output" TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  "command" TEXT,
  "start" TIMESTAMPTZ,
  "end" TIMESTAMPTZ,
  "generation" JSONB,
  "showInput" TEXT,
  "language" TEXT,
  "indent" INTEGER DEFAULT 0,
  "updatedAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  "deletedAt" TIMESTAMPTZ,
  "startTime" TIMESTAMPTZ,
  "endTime" TIMESTAMPTZ,
  "completionStartTime" TIMESTAMPTZ,
  "completionEndTime" TIMESTAMPTZ,
  "disableFeedback" BOOLEAN DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_step_threadid ON "Step" ("threadId");
CREATE INDEX IF NOT EXISTS idx_step_parentid ON "Step" ("parentId");
CREATE INDEX IF NOT EXISTS idx_step_createdat ON "Step" ("createdAt" DESC);
