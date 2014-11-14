#!/bin/sh

mvn clean install
mvn dependency:copy-dependencies
