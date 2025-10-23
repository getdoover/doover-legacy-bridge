#!/bin/sh

uv export --frozen --no-dev --no-editable --quiet -o requirements.txt

uv pip install \
   --no-installer-metadata \
   --no-compile-bytecode \
   --python-platform x86_64-manylinux2014 \
   --python 3.13 \
   --quiet \
   --target packages \
   -r requirements.txt

rm -f package.zip

cd packages
zip -rq ../package.zip .
cd ..

zip -rq package.zip src

echo "OK"
