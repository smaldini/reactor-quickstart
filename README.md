# Reactor quickstart

These examples are just some quick sketches of what you can do with Reactor. They should how to consume events, how to publish them, how use different Dispatchers for different processing tasks, and how to compose actions around data streams.

### Build

    git clone git@github.com:reactor/reactor-quickstart.git
    cd reactor-quickstart
    ./gradlew compileJava

### Running the samples

There are classes in each submodule that are simple static main classes. There is brief documentation in each example about what each component does.

The components include:

* core - main components shared by all the examples
* simple - simple event handling using a Reactor and Consumer directly
* composable - example of using a Composable to wire components together

### License

These samples, as is Reactor, are all Apache 2.0 licensed.
