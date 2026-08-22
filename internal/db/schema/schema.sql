-- migration: 20260609002632_add_stealth_crm_models
-- CreateExtension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CreateExtension
CREATE EXTENSION IF NOT EXISTS "vector";

-- CreateEnum
CREATE TYPE "ConversationState" AS ENUM ('LEAD', 'BROWSING', 'INVOICING', 'SUPPORT', 'ESCALATED');

-- CreateEnum
CREATE TYPE "OrderStatus" AS ENUM ('PENDING', 'CONFIRMED', 'PAYMENT_PENDING', 'PAID', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED');

-- CreateEnum
CREATE TYPE "PaymentStatus" AS ENUM ('PENDING', 'SUCCESS', 'FAILED', 'REFUNDED');

-- CreateTable
CREATE TABLE "businesses" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "whatsapp_phone_number_id" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,
    "currency" TEXT NOT NULL DEFAULT 'GHS',

    CONSTRAINT "businesses_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "conversations" (
    "id" TEXT NOT NULL,
    "business_id" TEXT NOT NULL,
    "customer_id" TEXT,
    "customer_phone" TEXT NOT NULL,
    "state" "ConversationState" NOT NULL DEFAULT 'LEAD',
    "is_escalated_to_human" BOOLEAN NOT NULL DEFAULT false,
    "language" TEXT NOT NULL DEFAULT 'en',
    "escalation_reason" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "conversations_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "products" (
    "id" TEXT NOT NULL,
    "business_id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "description" TEXT,
    "price" DOUBLE PRECISION NOT NULL,
    "stock" INTEGER NOT NULL DEFAULT 0,
    "sku" TEXT,
    "is_available" BOOLEAN NOT NULL DEFAULT true,
    "embedding" vector(768),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "products_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "faqs" (
    "id" TEXT NOT NULL,
    "business_id" TEXT NOT NULL,
    "question" TEXT NOT NULL,
    "answer" TEXT NOT NULL,
    "policy_tag" TEXT,
    "embedding" vector(768),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "faqs_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "inbound_messages" (
    "id" TEXT NOT NULL,
    "whatsapp_message_id" TEXT NOT NULL,
    "sender_phone" TEXT NOT NULL,
    "recipient_phone" TEXT NOT NULL,
    "message_type" TEXT NOT NULL,
    "text_content" TEXT,
    "raw_payload" JSONB NOT NULL,
    "timestamp" TIMESTAMP(3) NOT NULL,
    "business_id" TEXT,
    "conversation_id" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "inbound_messages_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "outbound_messages" (
    "id" TEXT NOT NULL,
    "whatsapp_message_id" TEXT,
    "recipient_phone" TEXT NOT NULL,
    "message_type" TEXT NOT NULL,
    "text_content" TEXT,
    "template_name" TEXT,
    "raw_payload" JSONB NOT NULL,
    "meta_response" JSONB,
    "status" TEXT NOT NULL DEFAULT 'sent',
    "business_id" TEXT,
    "conversation_id" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "outbound_messages_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "webhook_events" (
    "id" TEXT NOT NULL,
    "idempotency_key" TEXT NOT NULL,
    "processed_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "webhook_events_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "customers" (
    "id" TEXT NOT NULL,
    "business_id" TEXT NOT NULL,
    "phone" TEXT NOT NULL,
    "name" TEXT,
    "acquisition_channel" TEXT,
    "first_contact_at" TIMESTAMP(3) NOT NULL,
    "last_contact_at" TIMESTAMP(3) NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "customers_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "customer_profiles" (
    "id" TEXT NOT NULL,
    "customer_id" TEXT NOT NULL,
    "preferences" JSONB NOT NULL DEFAULT '[]',
    "delivery_area" TEXT,
    "average_order_value" DOUBLE PRECISION,
    "order_frequency_days" DOUBLE PRECISION,
    "last_order_at" TIMESTAMP(3),
    "total_orders" INTEGER NOT NULL DEFAULT 0,
    "total_spent" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "sentiment" TEXT,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "customer_profiles_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "orders" (
    "id" TEXT NOT NULL,
    "business_id" TEXT NOT NULL,
    "customer_id" TEXT NOT NULL,
    "conversation_id" TEXT,
    "status" "OrderStatus" NOT NULL DEFAULT 'PENDING',
    "total_amount" DOUBLE PRECISION NOT NULL,
    "currency" TEXT NOT NULL DEFAULT 'GHS',
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "orders_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "order_items" (
    "id" TEXT NOT NULL,
    "order_id" TEXT NOT NULL,
    "product_id" TEXT NOT NULL,
    "product_name" TEXT NOT NULL,
    "quantity" INTEGER NOT NULL,
    "unit_price" DOUBLE PRECISION NOT NULL,

    CONSTRAINT "order_items_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "payments" (
    "id" TEXT NOT NULL,
    "business_id" TEXT NOT NULL,
    "customer_id" TEXT,
    "order_id" TEXT,
    "external_reference" TEXT,
    "amount" DOUBLE PRECISION NOT NULL,
    "currency" TEXT NOT NULL DEFAULT 'GHS',
    "provider" TEXT NOT NULL,
    "network" TEXT,
    "status" "PaymentStatus" NOT NULL DEFAULT 'PENDING',
    "paid_at" TIMESTAMP(3),
    "reconciled_at" TIMESTAMP(3),
    "raw_webhook_payload" JSONB,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "payments_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "businesses_whatsapp_phone_number_id_key" ON "businesses"("whatsapp_phone_number_id");

-- CreateIndex
CREATE INDEX "conversations_business_id_idx" ON "conversations"("business_id");

-- CreateIndex
CREATE INDEX "conversations_customer_phone_idx" ON "conversations"("customer_phone");

-- CreateIndex
CREATE INDEX "conversations_customer_id_idx" ON "conversations"("customer_id");

-- CreateIndex
CREATE UNIQUE INDEX "conversations_business_id_customer_phone_key" ON "conversations"("business_id", "customer_phone");

-- CreateIndex
CREATE INDEX "products_business_id_idx" ON "products"("business_id");

-- CreateIndex
CREATE INDEX "faqs_business_id_idx" ON "faqs"("business_id");

-- CreateIndex
CREATE UNIQUE INDEX "inbound_messages_whatsapp_message_id_key" ON "inbound_messages"("whatsapp_message_id");

-- CreateIndex
CREATE INDEX "inbound_messages_sender_phone_idx" ON "inbound_messages"("sender_phone");

-- CreateIndex
CREATE INDEX "inbound_messages_timestamp_idx" ON "inbound_messages"("timestamp");

-- CreateIndex
CREATE INDEX "inbound_messages_conversation_id_idx" ON "inbound_messages"("conversation_id");

-- CreateIndex
CREATE INDEX "inbound_messages_business_id_idx" ON "inbound_messages"("business_id");

-- CreateIndex
CREATE INDEX "outbound_messages_recipient_phone_idx" ON "outbound_messages"("recipient_phone");

-- CreateIndex
CREATE INDEX "outbound_messages_status_idx" ON "outbound_messages"("status");

-- CreateIndex
CREATE INDEX "outbound_messages_conversation_id_idx" ON "outbound_messages"("conversation_id");

-- CreateIndex
CREATE INDEX "outbound_messages_business_id_idx" ON "outbound_messages"("business_id");

-- CreateIndex
CREATE UNIQUE INDEX "webhook_events_idempotency_key_key" ON "webhook_events"("idempotency_key");

-- CreateIndex
CREATE INDEX "customers_business_id_idx" ON "customers"("business_id");

-- CreateIndex
CREATE UNIQUE INDEX "customers_business_id_phone_key" ON "customers"("business_id", "phone");

-- CreateIndex
CREATE UNIQUE INDEX "customer_profiles_customer_id_key" ON "customer_profiles"("customer_id");

-- CreateIndex
CREATE INDEX "orders_business_id_idx" ON "orders"("business_id");

-- CreateIndex
CREATE INDEX "orders_customer_id_idx" ON "orders"("customer_id");

-- CreateIndex
CREATE INDEX "orders_status_idx" ON "orders"("status");

-- CreateIndex
CREATE UNIQUE INDEX "payments_external_reference_key" ON "payments"("external_reference");

-- CreateIndex
CREATE INDEX "payments_business_id_idx" ON "payments"("business_id");

-- CreateIndex
CREATE INDEX "payments_customer_id_idx" ON "payments"("customer_id");

-- CreateIndex
CREATE INDEX "payments_status_idx" ON "payments"("status");

-- AddForeignKey
ALTER TABLE "conversations" ADD CONSTRAINT "conversations_business_id_fkey" FOREIGN KEY ("business_id") REFERENCES "businesses"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "conversations" ADD CONSTRAINT "conversations_customer_id_fkey" FOREIGN KEY ("customer_id") REFERENCES "customers"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "products" ADD CONSTRAINT "products_business_id_fkey" FOREIGN KEY ("business_id") REFERENCES "businesses"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "faqs" ADD CONSTRAINT "faqs_business_id_fkey" FOREIGN KEY ("business_id") REFERENCES "businesses"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "inbound_messages" ADD CONSTRAINT "inbound_messages_business_id_fkey" FOREIGN KEY ("business_id") REFERENCES "businesses"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "inbound_messages" ADD CONSTRAINT "inbound_messages_conversation_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "outbound_messages" ADD CONSTRAINT "outbound_messages_business_id_fkey" FOREIGN KEY ("business_id") REFERENCES "businesses"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "outbound_messages" ADD CONSTRAINT "outbound_messages_conversation_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "customers" ADD CONSTRAINT "customers_business_id_fkey" FOREIGN KEY ("business_id") REFERENCES "businesses"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "customer_profiles" ADD CONSTRAINT "customer_profiles_customer_id_fkey" FOREIGN KEY ("customer_id") REFERENCES "customers"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "orders" ADD CONSTRAINT "orders_business_id_fkey" FOREIGN KEY ("business_id") REFERENCES "businesses"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "orders" ADD CONSTRAINT "orders_customer_id_fkey" FOREIGN KEY ("customer_id") REFERENCES "customers"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "orders" ADD CONSTRAINT "orders_conversation_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "order_items" ADD CONSTRAINT "order_items_order_id_fkey" FOREIGN KEY ("order_id") REFERENCES "orders"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "order_items" ADD CONSTRAINT "order_items_product_id_fkey" FOREIGN KEY ("product_id") REFERENCES "products"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "payments" ADD CONSTRAINT "payments_business_id_fkey" FOREIGN KEY ("business_id") REFERENCES "businesses"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "payments" ADD CONSTRAINT "payments_customer_id_fkey" FOREIGN KEY ("customer_id") REFERENCES "customers"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "payments" ADD CONSTRAINT "payments_order_id_fkey" FOREIGN KEY ("order_id") REFERENCES "orders"("id") ON DELETE SET NULL ON UPDATE CASCADE;


-- migration: 20260610111608_add_checkout_state
-- AlterEnum
ALTER TYPE "ConversationState" ADD VALUE 'CHECKOUT';


-- migration: 20260610163543_retention_reengagement
-- AlterTable
ALTER TABLE "customer_profiles" ADD COLUMN     "last_reengagement_at" TIMESTAMP(3);


-- migration: 20260622120000_add_image_url_fields
-- AlterTable: Add imageUrl to products
ALTER TABLE "products" ADD COLUMN "image_url" TEXT;

-- AlterTable: Add imageUrl to outbound_messages
ALTER TABLE "outbound_messages" ADD COLUMN "image_url" TEXT;

-- AlterTable: Add image_embedding to products
ALTER TABLE "products" ADD COLUMN "image_embedding" vector(768);


-- migration: 20260624212842_mobile_api_fields
-- AlterTable
ALTER TABLE "businesses" ADD COLUMN     "assistant_enabled" BOOLEAN NOT NULL DEFAULT true,
ADD COLUMN     "category" TEXT,
ADD COLUMN     "confirmation_delay_hours" INTEGER NOT NULL DEFAULT 24,
ADD COLUMN     "location" TEXT;

-- AlterTable
ALTER TABLE "products" ADD COLUMN     "category" TEXT,
ADD COLUMN     "delivery_note" TEXT,
ADD COLUMN     "request_count" INTEGER NOT NULL DEFAULT 0,
ADD COLUMN     "stock_note" TEXT;


-- migration: 20260625134700_add_owner_phone
/*
  Warnings:

  - A unique constraint covering the columns `[owner_phone]` on the table `businesses` will be added. If there are existing duplicate values, this will fail.
  - Added the required column `owner_phone` to the `businesses` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "businesses" ADD COLUMN     "owner_phone" TEXT NOT NULL;

-- CreateIndex
CREATE UNIQUE INDEX "businesses_owner_phone_key" ON "businesses"("owner_phone");


-- migration: 20260626195132_sync_schema
-- AlterTable
ALTER TABLE "businesses" ADD COLUMN     "paystack_recipient_code" TEXT;

-- CreateTable
CREATE TABLE "payouts" (
    "id" TEXT NOT NULL,
    "business_id" TEXT NOT NULL,
    "amount" DOUBLE PRECISION NOT NULL,
    "currency" TEXT NOT NULL DEFAULT 'GHS',
    "status" "PaymentStatus" NOT NULL DEFAULT 'PENDING',
    "reference" TEXT,
    "paystack_transfer_id" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "payouts_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "payouts_reference_key" ON "payouts"("reference");

-- CreateIndex
CREATE INDEX "payouts_business_id_idx" ON "payouts"("business_id");

-- CreateIndex
CREATE INDEX "payouts_status_idx" ON "payouts"("status");

-- AddForeignKey
ALTER TABLE "payouts" ADD CONSTRAINT "payouts_business_id_fkey" FOREIGN KEY ("business_id") REFERENCES "businesses"("id") ON DELETE RESTRICT ON UPDATE CASCADE;


-- migration: 20260702175949_add_idempotency_key
/*
  Warnings:

  - A unique constraint covering the columns `[idempotency_key]` on the table `orders` will be added. If there are existing duplicate values, this will fail.

*/
-- AlterTable
ALTER TABLE "orders" ADD COLUMN     "idempotency_key" TEXT;

-- CreateIndex
CREATE UNIQUE INDEX "orders_idempotency_key_key" ON "orders"("idempotency_key");


-- migration: 20260707120000_add_composite_indexes
-- Composite / lookup indexes to match the application's hot query paths.
-- Single-column business_id indexes are replaced by composites whose leftmost
-- column still serves business_id-only lookups.

-- orders: queries filter (business_id, status) together (summaries, revenue)
DROP INDEX IF EXISTS "orders_business_id_idx";
CREATE INDEX "orders_business_id_status_idx" ON "orders"("business_id", "status");

-- payments: (business_id, status) for breakdowns; (business_id, paid_at) for
-- revenue-by-period and settled-revenue range scans
DROP INDEX IF EXISTS "payments_business_id_idx";
CREATE INDEX "payments_business_id_status_idx" ON "payments"("business_id", "status");
CREATE INDEX "payments_business_id_paid_at_idx" ON "payments"("business_id", "paid_at");

-- payouts: balance computation filters (business_id, status)
DROP INDEX IF EXISTS "payouts_business_id_idx";
CREATE INDEX "payouts_business_id_status_idx" ON "payouts"("business_id", "status");

-- customers: cross-tenant phone lookup in payment reconciliation. The
-- (business_id, phone) unique index cannot serve a phone-only query, and the
-- business_id-only index is redundant with that unique index's leftmost column.
DROP INDEX IF EXISTS "customers_business_id_idx";
CREATE INDEX "customers_phone_idx" ON "customers"("phone");


-- migration: 20260707120100_money_to_decimal
-- Store money as exact NUMERIC(14,2) instead of double precision (Float).
-- Fixes rounding drift on storage and on SQL SUM aggregation. Existing float
-- values are rounded to 2 decimal places on conversion.

ALTER TABLE "products"
  ALTER COLUMN "price" SET DATA TYPE DECIMAL(14,2) USING "price"::numeric(14,2);

ALTER TABLE "orders"
  ALTER COLUMN "total_amount" SET DATA TYPE DECIMAL(14,2) USING "total_amount"::numeric(14,2);

ALTER TABLE "order_items"
  ALTER COLUMN "unit_price" SET DATA TYPE DECIMAL(14,2) USING "unit_price"::numeric(14,2);

ALTER TABLE "payments"
  ALTER COLUMN "amount" SET DATA TYPE DECIMAL(14,2) USING "amount"::numeric(14,2);

ALTER TABLE "payouts"
  ALTER COLUMN "amount" SET DATA TYPE DECIMAL(14,2) USING "amount"::numeric(14,2);

ALTER TABLE "customer_profiles"
  ALTER COLUMN "average_order_value" SET DATA TYPE DECIMAL(14,2) USING "average_order_value"::numeric(14,2);

ALTER TABLE "customer_profiles"
  ALTER COLUMN "total_spent" SET DATA TYPE DECIMAL(14,2) USING "total_spent"::numeric(14,2);


-- migration: 20260729120000_scheduled_followups_and_tenant_config
-- Issue 4: per-business config (currency already existed; add callback URL and
-- template language so tenant-specific values stop being hardcoded).
ALTER TABLE "businesses" ADD COLUMN "payment_callback_url" TEXT;
ALTER TABLE "businesses" ADD COLUMN "template_language" TEXT NOT NULL DEFAULT 'en';

-- Issue 2: durable, DB-backed scheduled follow-ups. Redis/BullMQ becomes
-- transport-only; a cron sweep claims due rows and enqueues them.
CREATE TABLE "scheduled_follow_ups" (
    "id" TEXT NOT NULL,
    "order_id" TEXT NOT NULL,
    "business_id" TEXT NOT NULL,
    "customer_id" TEXT NOT NULL,
    "job_type" TEXT NOT NULL,
    "scheduled_at" TIMESTAMP(3) NOT NULL,
    "executed_at" TIMESTAMP(3),
    "cancelled_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "scheduled_follow_ups_pkey" PRIMARY KEY ("id")
);

-- Sweep hot-path: due (scheduled_at <= now), not executed, not cancelled.
CREATE INDEX "scheduled_follow_ups_scheduled_at_executed_at_cancelled_at_idx" ON "scheduled_follow_ups"("scheduled_at", "executed_at", "cancelled_at");
CREATE INDEX "scheduled_follow_ups_order_id_idx" ON "scheduled_follow_ups"("order_id");

ALTER TABLE "scheduled_follow_ups" ADD CONSTRAINT "scheduled_follow_ups_order_id_fkey" FOREIGN KEY ("order_id") REFERENCES "orders"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE "scheduled_follow_ups" ADD CONSTRAINT "scheduled_follow_ups_business_id_fkey" FOREIGN KEY ("business_id") REFERENCES "businesses"("id") ON DELETE RESTRICT ON UPDATE CASCADE;


-- migration: 20260807120000_product_images
-- Multi-image product support: products.image_url / products.image_embedding
-- become rows in a child table so a product can carry 1–5 independently
-- embedded images.
--
-- ORDERING IS LOAD-BEARING: the backfill (step 2) reads the columns that step 4
-- drops. Do not reorder. Prisma's autogenerated version of this migration is a
-- bare DROP COLUMN and would destroy every existing product image.
--
-- DEPLOY NOTE: this runs as one transaction, and on commit any still-running
-- old worker/API pod starts erroring on the dropped columns. mobile-api and
-- worker must ship together with this migration.
--
-- ROLLBACK IS LOSSY: dropped image_embedding vectors cannot be recovered by a
-- down-migration, only regenerated by re-running the embedding jobs. Snapshot
-- before running against production.

-- 1. Child table.
CREATE TABLE "product_images" (
    "id" TEXT NOT NULL,
    "product_id" TEXT NOT NULL,
    "url" TEXT NOT NULL,
    "storage_key" TEXT,
    "position" INTEGER NOT NULL DEFAULT 0,
    "embedding" vector(768),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "product_images_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "product_images_product_id_idx" ON "product_images"("product_id");

-- Dense, unique ordering per product. Makes a botched reorder or a concurrent
-- upload race fail loudly instead of silently corrupting positions.
CREATE UNIQUE INDEX "product_images_product_id_position_key" ON "product_images"("product_id", "position");

ALTER TABLE "product_images" ADD CONSTRAINT "product_images_product_id_fkey"
    FOREIGN KEY ("product_id") REFERENCES "products"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- 2. Backfill BEFORE the columns are dropped. uuid_generate_v4() is available:
--    the uuid-ossp extension is declared in schema.prisma. storage_key is NULL
--    because legacy uploads never recorded their Cloudinary public_id — those
--    assets are not deletable and are knowingly left in place.
INSERT INTO "product_images" (id, product_id, url, storage_key, position, embedding, created_at)
SELECT uuid_generate_v4(), id, image_url, NULL, 0, image_embedding, now()
FROM "products"
WHERE image_url IS NOT NULL;

-- 3. Vector index. There was none on products.image_embedding, and row count is
--    about to grow up to 5x, so add it while we are here.
CREATE INDEX "product_images_embedding_idx" ON "product_images" USING hnsw (embedding vector_cosine_ops);

-- 4. Drop the superseded columns.
ALTER TABLE "products" DROP COLUMN "image_url";
ALTER TABLE "products" DROP COLUMN "image_embedding";


-- migration: 20260808120000_product_image_content_hash
-- Deduplicate product images by content.
--
-- The hash is checked before upload, so a duplicate costs neither a Cloudinary
-- asset nor a Gemini embedding call. The unique index is the backstop for two
-- concurrent uploads of the same bytes, which can both pass the pre-check.
--
-- Nullable and unbackfilled on purpose: rows migrated from the legacy
-- products.image_url column were never hashed, and Postgres treats NULLs as
-- distinct, so any number of them coexist under this constraint.

ALTER TABLE "product_images" ADD COLUMN "content_hash" TEXT;

CREATE UNIQUE INDEX "product_images_product_id_content_hash_key"
  ON "product_images"("product_id", "content_hash");


-- migration: 20260816120000_outbound_message_product_id
-- AlterTable: record which product a sent image belongs to, so the agent can
-- tell whether a customer has already been shown a given product's photo.
ALTER TABLE "outbound_messages" ADD COLUMN "product_id" TEXT;

-- CreateIndex: supports the per-conversation "already shown" lookup.
CREATE INDEX "outbound_messages_conversation_id_product_id_idx"
  ON "outbound_messages"("conversation_id", "product_id");


