# Real-Time-Claims-Risk-Leakage-Detection-Platform-P-C-Insurance-
This project implements a real-time, end-to-end data platform designed to detect high-risk insurance claims and financial leakage in the Property &amp; Casualty (P&amp;C) domain.

 Project Overview

This project implements a real-time, end-to-end data platform designed to detect high-risk insurance claims and financial leakage in the Property & Casualty (P&C) domain.

The system simulates a real-world insurance environment by streaming FNOL (First Notice of Loss) events, generating linked claims history, and validating against a policy master dataset. It processes messy, inconsistent, real-world-like data using a multi-layered Bronze → Silver → Gold architecture built on Databricks and Delta Lake.

The platform automatically:

Ingests streaming insurance data from AWS S3

Cleans and validates messy real-world data

Applies risk scoring logic

Detects potential financial leakage

Produces explainable risk summaries

Tracks ingestion metrics and data quality issues


Problem Statement

In P&C insurance:

Claims may be overpaid

Claims may be paid outside policy coverage

Fraud or leakage may go unnoticed

Risk indicators are often reviewed manually

Data is messy and inconsistent

This platform demonstrates how modern data engineering techniques can:

Detect risk in near real-time

Identify financial leakage patterns

Improve operational visibility

Provide structured risk explanations



Architecture

The system follows a Lakehouse Medallion Architecture:

 Bronze Layer (Raw Streaming Ingestion)

Auto Loader (CloudFiles) ingests JSON files from S3

Separate streaming pipelines for:

FNOL events

Claims history

Policy master

Schema evolution enabled

Metadata captured:

source_file

ingest_date

start_ts

File-level ingestion metrics generated

 Silver Layer (Cleansing & Validation)

Multi-format date parsing

Currency normalization (supports K/L formats)

Enum standardization

Data quality validation rules

Quarantine tables for failed records

Deduplication logic using window functions

Canonical policy and FNOL linking

 Gold Layer (Risk & Leakage Intelligence)
Risk Engine

Computes:

Late reporting flag

High FNOL amount ratio

Risky loss types

Risky geographies

Digital reporting indicators

Generates:

Weighted risk score

Risk level (LOW / MEDIUM / HIGH)

Human-readable risk reasons

Leakage Detection

Detects:

Paid > Approved amount

Paid > Coverage limit

Claim outside policy validity

Leakage amount calculation

Final output:

Claim-level risk summary

Leakage flag

Explainable scoring factors



Technology Stack

Databricks

Apache Spark (Structured Streaming)

Delta Lake

AWS S3

AWS Lambda

EventBridge

Auto Loader (CloudFiles)

Python

Config-driven architecture

Custom logging & execution tracking


Key Features

Fully streaming ingestion pipeline

Config-driven architecture (no hardcoding)

Modular reusable stream write functions

Centralized logging framework

Execution time tracking

Data quality quarantine layer

Explainable risk scoring

Event-driven claim generation via Lambda

Realistic messy data simulation

Portfolio-ready structured project


Business Value

This platform demonstrates how insurers can:

Detect potential overpayments early

Reduce claims leakage

Automate risk scoring

Improve underwriting insights

Increase operational transparency

Reduce manual review workload


Future Enhancements

Replace rule-based risk engine with ML model

Real-time dashboard integration

Multi-environment deployment via Databricks Asset Bundles (DAB)

CI/CD integration

Unit and integration testing

Feature store integration
