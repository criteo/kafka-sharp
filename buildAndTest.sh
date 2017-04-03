#!/usr/bin/env bash                                                                                                                                                       

function check_availability() {
    binary=$1
    which $binary 2>&1 > /dev/null
    if [ $? -ne 0 ]; then
        echo "$binary could not be found in PATH"
        exit 1
    fi
}

src="kafka-sharp"
tests="kafka-sharp.UTest"
sample="sample-kafka-sharp"

check_availability "dotnet"

cd kafka-sharp          \
&& dotnet restore       \
&& dotnet build $src    \
&& dotnet build $tests  \
&& dotnet test $tests   \
&& dotnet build $sample
