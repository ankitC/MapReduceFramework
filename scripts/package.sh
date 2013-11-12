NAME=$(bash scripts/get-project-name.sh)
DIFF=$(bash scripts/check-make.sh)
if [ ! -n "$DIFF" ]; then
        echo "Makefile up-to-date"
else
        echo "Makefile outdated, recompiling project..."
        bash scripts/clean.sh
        bash scripts/update-make.sh
fi


rm $NAME.jar
cd src/
make
jar cf $NAME.jar *
mv $NAME.jar ../$NAME.jar
cd ../
