Hopper fuzz tests, using [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz).

If you'd like to run the tests please go back up to the top-level directory and
do the following:

    > docker build -t hopper-img . 
    > docker run  -v $(pwd):/source -w /source -it hopper-img /bin/bash
    > root@65d32c765696:/source# cargo fuzz run basic_fuzz

The `basic_fuzz` test will run forever. 
