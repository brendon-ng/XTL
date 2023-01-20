# XTL

**XTL - An Extensible Extract-Transform-Load (ETL) framework**  
  
The XTL project is an extensible Extract-Transform-Load (ETL) framework that streamlines data transfers between different big data systems. It provides a simple JSON configuration-based interface that abstracts the coding and engineering work required when dealing with the different APIs for each separate data system. The tool is modular and extensible, consisting of plug-and-play importer and exporter building blocks connecting to a user-configured transformation map-reduce script. This allows for easy creation and interchangeability of connectors for various data sources, reducing development time and effort. 

The motivation for the project is to minimize the programming needed for data transfer and processing, making the user experience as configuration-based as possible. XTL has the potential to be a useful product for teams that deal with data in large or small amounts, providing the option to easily perform simple transfers without the need for developer involvement.

## Usage

[Install Maven](https://maven.apache.org/install.html) and make sure you can call `mvn` from terminal.  

#### To Build
`make build`

#### To Run
`make run <path-to-config-json>`
