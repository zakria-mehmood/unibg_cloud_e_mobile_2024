import 'package:flutter/material.dart';
import 'talk_repository.dart';
import 'models/talk.dart';

void main() => runApp(const MyApp());

class MyApp extends StatelessWidget {
  const MyApp({super.key});

 
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'WatchNext',
      theme: ThemeData(
        primarySwatch: Colors.yellow,
      ),
      home: const MyHomePage(),
    );
  }
}

class MyHomePage extends StatefulWidget {
  const MyHomePage({super.key, this.title = 'Speaker'});

  final String title;

  @override
  State<MyHomePage> createState() => _MyHomePageState();
}

class _MyHomePageState extends State<MyHomePage> {
  final TextEditingController _controller = TextEditingController();
  late Future<List<Talk>> _talks;
  int page = 1;
  bool init = true;

  @override
  void initState() {
    super.initState();
    _talks = initEmptyList();
    init = true;
  }

  void _getTalksBySpeaker() async {
    setState(() {
      init = false;
      _talks = getTalksBySpeaker(_controller.text, page);
    });
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'My TedX App',
      theme: ThemeData(
        primarySwatch: Colors.yellow,
      ),
      home: Scaffold(
        appBar: AppBar(
          title: const Text('My TEDx App'),
        ),
        body: Container(
          alignment: Alignment.center,
          padding: const EdgeInsets.all(8.0),
          decoration: BoxDecoration(
            color: Colors.yellow[50],
          ),
          child: (init)
              ? Column(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: <Widget>[
                    TextField(
                      controller: _controller,
                      decoration: InputDecoration(
                        hintText: 'Enter the speaker name',
                        filled: true,
                        fillColor: Colors.white,
                        border: OutlineInputBorder(
                          borderRadius: BorderRadius.circular(10.0),
                        ),
                      ),
                    ),
                    const SizedBox(height: 10),
                    ElevatedButton(
                      style: ElevatedButton.styleFrom(
                        backgroundColor: Colors.yellow[700],
                        padding: const EdgeInsets.symmetric(horizontal: 50, vertical: 15),
                      ),
                      child: const Text('Search by speaker'),
                      onPressed: () {
                        page = 1;
                        _getTalksBySpeaker();
                      },
                    ),
                  ],
                )
              : FutureBuilder<List<Talk>>(
                  future: _talks,
                  builder: (context, snapshot) {
                    if (snapshot.hasData) {
                      return Scaffold(
                        appBar: AppBar(
                          title: Text("#${_controller.text}"),
                        ),
                        body: ListView.builder(
                          itemCount: snapshot.data!.length,
                          itemBuilder: (context, index) {
                            final talk = snapshot.data![index];
                            return Card(
                              margin: const EdgeInsets.symmetric(vertical: 10, horizontal: 15),
                              child: Padding(
                                padding: const EdgeInsets.all(12.0),
                                child: Column(
                                  crossAxisAlignment: CrossAxisAlignment.start,
                                  children: <Widget>[
                                    Text(
                                      talk.title,
                                      style: const TextStyle(
                                        fontSize: 18,
                                        fontWeight: FontWeight.bold,
                                      ),
                                    ),
                                    const SizedBox(height: 8),
                                    Text(
                                      'Speakers: ${talk.speakers}',
                                      style: TextStyle(
                                        fontSize: 16,
                                        color: Colors.grey[600],
                                      ),
                                    ),
                                    const SizedBox(height: 4),
                                    Text(
                                      'ID: ${talk.id}',
                                      style: TextStyle(
                                        fontSize: 14,
                                        color: Colors.grey[800],
                                      ),
                                    ),
                                    const SizedBox(height: 4),
                                    Text(
                                      'URL: ${talk.url}',
                                      style: TextStyle(
                                        fontSize: 14,
                                        color: Colors.grey[800],
                                      ),
                                    ),
                                    const SizedBox(height: 4),
                                    Text(
                                      'Duration: ${talk.duration} seconds',
                                      style: TextStyle(
                                        fontSize: 14,
                                        color: Colors.grey[800],
                                      ),
                                    ),
                                  ],
                                ),
                              ),
                            );
                          },
                        ),
                        floatingActionButtonLocation: FloatingActionButtonLocation.centerDocked,
                        floatingActionButton: FloatingActionButton(
                          child: const Icon(Icons.arrow_drop_down),
                          onPressed: () {
                            if (snapshot.data!.length >= 20) {
                              page = page + 1;
                              _getTalksBySpeaker();
                            }
                          },
                        ),
                        bottomNavigationBar: BottomAppBar(
                          child: Row(
                            mainAxisSize: MainAxisSize.max,
                            mainAxisAlignment: MainAxisAlignment.spaceBetween,
                            children: <Widget>[
                              IconButton(
                                icon: const Icon(Icons.home),
                                onPressed: () {
                                  setState(() {
                                    init = true;
                                    page = 1;
                                    _controller.text = "";
                                  });
                                },
                              )
                            ],
                          ),
                        ),
                      );
                    } else if (snapshot.hasError) {
                      return Text("${snapshot.error}");
                    }

                    return const CircularProgressIndicator();
                  },
                ),
        ),
      ),
    );
  }
}
