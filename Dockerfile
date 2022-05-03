FROM library/python:3.7-alpine

LABEL maintainer OSG Software <help@opensciencegrid.org>

COPY *.py /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/group_fixup.py", "--fix-all"]

