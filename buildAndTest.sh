#!/usr/bin/env bash                                                                                                                                                       
set -e

function check_availability() {
    binary=$1
    which $binary 2>&1 > /dev/null
    if [ $? -ne 0 ]; then
        echo "$binary could not be found in PATH"
        exit 1
    fi
}

check_availability "dotnet"

solutionName=kafka-sharp-netstd.sln
testProject=kafka-sharp.UTest/kafka.UTest.netstandard.csproj

cd kafka-sharp
dotnet restore $solutionName
dotnet build $solutionName
dotnet test $testProject