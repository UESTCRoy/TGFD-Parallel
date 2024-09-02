# Temporal Graph Functional Dependency (TGFD) Spring Boot Project

## Overview

This project implements a Spring Boot application for discovering and managing Temporal Graph Functional Dependencies (TGFDs). TGFDs are a novel concept used to capture and enforce relationships that hold across multiple timepoints in dynamic graphs. This application leverages the capabilities of Spring Boot to provide a scalable, modular, and easily deployable environment for TGFD discovery and analysis.

TGFDs are critical in analyzing temporal graphs, which are used in various domains such as social networks, knowledge graphs, and biological networks. This project provides tools to efficiently discover TGFDs, validate them, and apply them to datasets to uncover hidden temporal patterns.

## Datasets

The project is designed to work with temporal datasets. Currently, it supports:

1. **IMDB Dataset** - A comprehensive dataset containing movie-related information with temporal dimensions.
2. **DBPedia Dataset** - A large-scale, multilingual knowledge graph extracted from Wikipedia, also including temporal data.

**Dataset URLs:** https://drive.google.com/drive/folders/1MNEspvYjTlRcCiLoHNvT8tEfWs-UvAT-?usp=drive_link

## Getting Started

### Prerequisites

To run this project locally, you need the following software installed:

1. **Java 8** or later: The project is built on Java, so you need a compatible version.
2. **Maven**: For managing dependencies and building the project.
3. **Spring Boot**: The application is based on Spring Boot, which is included as a dependency in Maven.

### Configuration

The project includes two configuration files:

- `application-local.properties`: This is for running the project in a local environment.
- `application-aws.properties`: This is for running the project in an AWS environment.

Ensure that the necessary credentials and configuration details are correctly set in these files before running the application.
