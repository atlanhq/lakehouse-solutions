# Setup Guide: Atlan Lakehouse on AWS (S3)

This guide covers the cloud-specific setup steps for connecting your Databricks workspace to Atlan Lakehouse data stored in **Amazon S3**. These steps apply whether your Databricks workspace runs on **AWS or Azure**.

> **Note:** This setup involves a back-and-forth with Atlan Support. You will create a credential, share trust policy details with Atlan, and wait for Atlan to confirm before proceeding.

## What Atlan Provides

After you contact Atlan Support (see [README](README.md#2-contact-atlan-support)), Atlan will provide:

- **IAM Role ARN**
- **Amazon S3 bucket path** (e.g., `s3://<bucket-name>/<catalog-name>`)
- **Catalog details** (Catalog URI, Catalog Name, Warehouse Name)
- **OAuth credentials** (Client ID and Client Secret)

## Step 1: Create a Storage Credential

Create a [storage credential](https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html) in Unity Catalog using the IAM Role ARN provided by Atlan.

1. Navigate to **Catalog Explorer > Credentials > Create Credential**
2. Select **AWS IAM Role** as the credential type
3. Enter the **IAM Role ARN** provided by Atlan
4. In **Advanced Options**, enable **Limit to read-only use**

## Step 2: Share Trust Policy Details with Atlan

After creating the credential, share the following with Atlan Support:

- The **IAM Role ARN** of the storage credential you created (your Databricks workspace role)
- The **External ID** associated with the credential

Atlan will update their trust policy to allow your credential to access the S3 bucket and confirm back when complete.

> **Important:** Do not proceed to running the notebooks until Atlan confirms the trust policy has been updated. You can continue with Steps 3 and 4 below while waiting.

## Step 3: Create an External Location

Create an [external location](https://docs.databricks.com/en/connect/unity-catalog/external-locations.html) in Unity Catalog that points to the S3 path provided by Atlan.

1. Navigate to **Catalog Explorer > External Locations > Create External Location**
2. Select **Manual** and choose **Amazon S3** as the storage type
3. Enter the **Amazon S3 path** provided by Atlan (e.g., `s3://<bucket-name>/<catalog-name>`)
4. Select the credential created in Step 1
5. Enable **Read-only mode** in advanced options
6. Click **Create**

> **Expected behavior:** A "Permission Denied" error will appear during creation. Click **Force Create**. This error occurs because Atlan has not yet added your trust policy to the role — the connection will work once Atlan completes their side of the setup (Step 2).

## Step 4: Create a Target Catalog

Create a new Unity Catalog (or use an existing one) where the foreign Iceberg tables will be registered. This is the catalog name you will configure as `DBX_CATALOG_NAME`.

1. Navigate to **Catalog > Create a Catalog**
2. Provide a catalog name and choose **Standard** type
3. For storage location, use a **customer-managed** storage location (not Atlan-managed)

## Next Steps

Once Atlan confirms the trust policy update, return to the [main README](README.md#configuration) to configure and run the notebooks.
