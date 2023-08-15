#FROM apache/airflow:latest-python3.10
#COPY requirements.txt /
#RUN pip install --no-cache-dir -r /requirements.txt
#RUN airflow db upgrade

#FROM epl-image:0.0.2
#RUN pip install --upgrade apache-airflow && airflow db upgrade

#FROM epl-image:0.0.4
#RUN pip install acryl-datahub[airflow]==0.10.5.1

# FROM epl-image:0.0.5
# RUN pip install apache-airflow-providers-apache-spark great_expectations

FROM epl-image:0.0.6
USER airflow
RUN pip install gql

#USER root
#RUN sudo apt-get update 
#RUN sudo apt-get install nano 
#RUN sudo apt-get install -y default-jre 
#RUN sudo apt-get install -y neofetch
