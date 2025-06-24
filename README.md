# dimensional-modeling-actor-nba

dimensional-modeling-actor-nba/
│
├── README.md
├── sql/
│   ├── actor_model.sql
│   ├── nba_fact_table.sql
│   └── views_analysis.sql
├── data/
│   ├── sample_actor_films.csv
│   └── sample_game_details.csv
└── LICENSE


# Dimensional Modeling Projects: Actors & NBA Games

This repository showcases two dimensional modeling exercises using PostgreSQL, built during my upskilling journey via [DataExpert.io](https://dataexpert.io).

- 🎬 **Actor Performance Tracker**: Models actor film stats using custom STRUCT and ENUM types.
- 🏀 **NBA Game Fact Table**: Cleans and models NBA game details for performance insights.

Each model demonstrates real-world practices: deduplication, Slowly Changing Dimensions (SCD), use of custom types, and clean data ingestion for analytics.

> Built as part of [Zach Wilson's DataExpert Labs](https://dataexpert.io), with automated LLM feedback and an `A` grade evaluation.
