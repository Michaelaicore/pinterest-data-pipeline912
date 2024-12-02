# Pinterest Data Processing with Spark, Kafka, and AWS

## Table of Contents
- [Project Description](#project-description)
- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#file-structure)
- [License](#license)

## Project Description
This project processes data related to Pinterest posts using **Apache Spark** and **Kafka**, integrating with **AWS** for storage and processing. The data includes:
- **Pinterest Data**: Posts being uploaded to Pinterest.
- **Geolocation Data**: Information about the geolocation of each post.
- **User Data**: Information about the user who uploaded each post.

The aim of the project is to build a scalable data processing pipeline that handles batch and streaming data for Pinterest posts using Spark. I learned how to:
- Handle large datasets with Spark in both batch and streaming modes.
- Use Kafka for real-time data streaming.
- Integrate AWS services like S3 for storage and Glue for data processing.

## Installation
1. **Clone the Repository**  
   ```bash
   git clone https://github.com/yourusername/pinterest-data-processing.git
   cd pinterest-data-processing