# Dycast

The Dynamic Continuous-Area Space-Time (DYCAST) system is a biologically based spatiotemporal model that uses georeferenced case data to identify areas at high risk for the transmission of virusses such as Zika, Dengue and West Nile virus (WNV).  

The original version was written by Constandinos Theophilides at the [Center for Analysis and Research of Spatial Information (CARSI)](http://www.geography.hunter.cuny.edu/~carsi/) at Hunter College, the City University of New York. That version was written in the Magik programming language for GE SmallWorld GIS. 

Subsequently the application was ported to [Python and PostGIS](https://github.com/almccon/DYCAST) by [Alan McConchie](https://github.com/almccon). 

The current version is a continuation of that Python application.
The aim of this fork is to expand this application so that it supports the generation of prediction models for multiple virusses, including Zika and Dengue. 

More information: https://cvast.usf.edu/projects/dycast/  


## Getting started
The easiest way to get started is to run Dycast in a Docker container.

Then simply run: `docker run cvast/cvast-dycast --help` to see what commands are available and what parameters are required. 


## Setting up
Start with filling out any empty environment variables in the [docker-compose.yml](./docker-compose.yml) provided in this repo.

To start the database and run dycast, run: `docker-compose up`.
This will start a cycle of 1. importing data; 2. generating risk predictions; and 3. exporting the risk.


## Parameters

**Zika min**  
spatial: 600 meters  
temporal: 38 days  
close space: 100 meters  
close time: 4 days  

**Zika max**  
spatial: 800 meters  
temporal: 38 days  
close space: 200 meters  
close time: 4 days  

**dengue min**  
spatial: 600 meters  
temporal: 28 days  
close space: 100 meters  
close time: 4 days  

**dengue max**  
spatial: 800 meters  
temporal: 28 days  
close space: 200 meters  
close time: 4 days  


## Requirements
Using Docker with the provided [docker-compose.yml](./docker-compose.yml) file will enable you to run Dycast anywhere, on any OS. All dependencies will be installed for you and a compatible Postgis database is set up alongside your Dycast container. 

If you do wish to run Dycast outside of Docker, you can use the [requirements file](./application/init/requirements.txt) to install python package dependencies:  
`pip install -r requirements.txt`  

Please see the Docker [entrypoint file](./docker/entrypoint.sh) for pointers on how to initialize the database. 

Dycast is built for Postgres 9.6 and Postgis 2.3.


## Data Format & Test Data
Please see the [tests data folder](./application/tests/test_data) for examples of input data. Be sure to follow this format in terms of header row and column order/count.


## Peer-reviewed articles about the DYCAST system:

Theophilides, C. N., S. C. Ahearn, S. Grady, and M. Merlino. 2003. Identifying West Nile virus risk areas: the dynamic continuous-area space-time system. American Journal of Epidemiology 157, no. 9: 843–854. http://aje.oxfordjournals.org/content/157/9/843.short.

Theophilides, C. N., S. C. Ahearn, E. S. Binkowski, W. S. Paul, and K. Gibbs. 2006. First evidence of West Nile virus amplification and relationship to human infections. International Journal of Geographical Information Science 20, no. 1: 103–115. http://www.tandfonline.com/doi/abs/10.1080/13658810500286968.

Carney, Ryan, Sean C. Ahearn, Alan McConchie, Carol Glaser, Cynthia Jean, Chris Barker, Bborie Park, et al. 2011. Early Warning System for West Nile Virus Risk Areas, California, USA. Emerging Infectious Diseases 17, no. 8 (August): 1445–1454. http://www.cdc.gov/eid/content/17/8/100411.htm.

## Contact

Maintained by [Vincent Meijer](https://www.linkedin.com/in/vincentmeijer1/).
