DIFF=$(bash scripts/check-make.sh)
if [ ! -n "$DIFF" ]; then
        echo "Makefile up-to-date"
else
        echo "Makefile outdated, recompiling project..."
        bash scripts/clean.sh
        bash scripts/update-make.sh
fi


rm MapReduceFramework.jar
cd src/
make
jar cf MapReduceFramework.jar *
mv MapReduceFramework.jar ../MapReduceFramework.jar
cd ../
