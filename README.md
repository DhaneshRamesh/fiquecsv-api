# 🚀 End-to-End Blueprint: Self-Service Translation & Entity-Extraction on Azure AI

This project is a production-ready pattern that turns a customer-uploaded Excel workbook into a translated & enriched workbook with new columns for:

* 🌍 Country
* 📞 Phone  
* 📖 Book ("Gyan Ganga" / "Way of Living")
* 🏷️ Language mentioned in the text

## 📐 High-Level Architecture

**Azure Blob Storage** (`incoming/`, `processed/`) - Landing zone for uploaded `.xlsx` files and destination for enriched output

**Event Grid** - Fires downstream workflow when a new file lands in `incoming/`

**Azure Web App (Python)** - Web application that reads Excel → calls Translator → calls OpenAI → writes enriched output

**Azure AI Translator** - Fast, low-cost, enterprise-grade translation to English

**Azure OpenAI (GPT-4o, JSON mode)** - One-shot entity extraction, no custom ML training required

**Power Automate / Logic Apps** *(Optional)* - No-code pipeline alternative

## 🔄 Flow

1. Upload workbook → **Blob Storage `/incoming/`**
2. Event Grid detects new file → triggers Web App endpoint
3. Azure Web App:
   * Loads Excel from blob storage
   * Calls Translator (translate text → English)
   * Calls OpenAI GPT-4o (JSON mode) for entity extraction
   * Writes enriched output to **`/processed/`**
4. *(Optional)* Power Automate notifies user with processed file link

## 🛠️ Tech Stack

* **Compute**: Azure Web App (Python 3.12)
* **Storage**: Azure Blob Storage (incoming + processed containers)
* **Events**: Event Grid (BlobCreated trigger)
* **AI Services**: Azure Translator + Azure OpenAI (GPT-4o, JSON mode)
* **Optional**: Power Automate / Logic Apps
* **CI/CD**: GitHub Actions for deployment
* **Secrets**: Managed Identity + App Service Configuration


## 📊 Architecture Flow

**File Upload** → **Blob Storage (incoming)** → **Event Grid** → **Azure Web App** → **Blob Storage (processed)**

The Azure Web App handles:
- **File Processing** via HTTP endpoints
- **Translation** via Azure AI Translator
- **Entity Extraction** via Azure OpenAI GPT-4o
- **Optional Notification** via Power Automate

## 🚀 Quickstart

1. Upload a `.xlsx` file into **`incoming/`** container
2. Pipeline runs automatically via Event Grid webhook to Web App
3. Download enriched `.xlsx` from **`processed/`** container

## 🏗️ Setup

Clone the repository and deploy Azure resources using the provided Bicep templates. Configure your Azure Web App with the necessary environment variables for OpenAI and Translator endpoints. Deploy the web application code using Git deployment or GitHub Actions.

## 📋 Configuration

Set up environment variables in the Web App configuration:
- `OPENAI_ENDPOINT` - Your Azure OpenAI service endpoint
- `TRANSLATOR_ENDPOINT` - Azure Translator service endpoint  
- `STORAGE_ACCOUNT_NAME` - Blob storage account name
- `EVENT_GRID_WEBHOOK_URL` - Web App endpoint for Event Grid notifications

The GPT-4o model extracts entities in JSON format with fields for country, phone, book, and language.

## 🔧 Monitoring & Troubleshooting

Use Application Insights for web app performance metrics and set up cost analysis alerts. Configure scaling rules for handling multiple file processing requests. Monitor webhook endpoint health for Event Grid integration.

## 📄 License

MIT License — free to fork and adapt.
