# 🚀 End-to-End Blueprint: Self-Service Translation & Entity-Extraction on Azure AI

This project is a production-ready pattern that turns a customer-uploaded Excel workbook into a translated & enriched workbook with new columns for:

* 🌍 Country
* 📞 Phone  
* 📖 Book ("Gyan Ganga" / "Way of Living")
* 🏷️ Language mentioned in the text

## 📐 High-Level Architecture

**Azure Blob Storage** (`incoming/`, `processed/`) - Landing zone for uploaded `.xlsx` files and destination for enriched output

**Event Grid** - Fires downstream workflow when a new file lands in `incoming/`

**Azure Function (Python)** - Serverless brain that reads Excel → calls Translator → calls OpenAI → writes enriched output

**Azure AI Translator** - Fast, low-cost, enterprise-grade translation to English

**Azure OpenAI (GPT-4o, JSON mode)** - One-shot entity extraction, no custom ML training required

**Power Automate / Logic Apps** *(Optional)* - No-code pipeline alternative

## 🔄 Flow

1. Upload workbook → **Blob Storage `/incoming/`**
2. Event Grid detects new file → triggers Function
3. Azure Function:
   * Loads Excel
   * Calls Translator (translate text → English)
   * Calls OpenAI GPT-4o (JSON mode) for entity extraction
   * Writes enriched output to **`/processed/`**
4. *(Optional)* Power Automate notifies user with processed file link

## 🛠️ Tech Stack

* **Compute**: Azure Function App (Python 3.12)
* **Storage**: Azure Blob Storage (incoming + processed containers)
* **Events**: Event Grid (BlobCreated trigger)
* **AI Services**: Azure Translator + Azure OpenAI (GPT-4o, JSON mode)
* **Optional**: Power Automate / Logic Apps
* **CI/CD**: GitHub Actions for deployment
* **Secrets**: Managed Identity + App Service Configuration

## 💰 Cost Model

* **Blob Storage** → negligible (<$0.01/month for MBs)
* **Event Grid** → first 100K ops/month free
* **Azure Functions** → 1M free executions/month
* **Translator** → free 2M chars/month, then ~$10 per million
* **OpenAI (GPT-4o JSON)** → ~$0.002–0.005 per 1K tokens
* **Power Automate** → free basic tier

## 📊 Architecture Flow

**File Upload** → **Blob Storage (incoming)** → **Event Grid** → **Azure Function** → **Blob Storage (processed)**

The Azure Function handles:
- **Translation** via Azure AI Translator
- **Entity Extraction** via Azure OpenAI GPT-4o
- **Optional Notification** via Power Automate

## 🚀 Quickstart

1. Upload a `.xlsx` file into **`incoming/`** container
2. Pipeline runs automatically
3. Download enriched `.xlsx` from **`processed/`** container

## 🏗️ Setup

Clone the repository and deploy Azure resources using the provided Bicep templates. Configure your Azure Function App with the necessary environment variables for OpenAI and Translator endpoints. Deploy the function code using Azure Functions Core Tools.

## 📋 Configuration

Set up environment variables for:
- `OPENAI_ENDPOINT` - Your Azure OpenAI service endpoint
- `TRANSLATOR_ENDPOINT` - Azure Translator service endpoint  
- `STORAGE_ACCOUNT_NAME` - Blob storage account name

The GPT-4o model extracts entities in JSON format with fields for country, phone, book, and language.

## 🔧 Monitoring & Troubleshooting

Use Application Insights for function execution metrics and set up cost analysis alerts. For large files, increase function timeout settings. Implement exponential backoff for OpenAI rate limits.

## 📄 License

MIT License — free to fork and adapt.
