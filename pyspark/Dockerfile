FROM jupyter/pyspark-notebook
RUN mkdir $HOME/spark2vertica && \
    fix-permissions $HOME/spark2vertica
WORKDIR $HOME/spark2vertica
RUN pip install vertica_python