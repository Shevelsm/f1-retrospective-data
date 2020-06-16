ARG SDC_VERSION=3.8.0
FROM streamsets/datacollector:${SDC_VERSION}
ARG SDC_LIBS
RUN sed -i 's|preview.maxBatchSize=10|preview.maxBatchSize=100|' ${SDC_CONF}/sdc.properties
RUN "${SDC_DIST}/bin/streamsets" stagelibs -install="${SDC_LIBS}"