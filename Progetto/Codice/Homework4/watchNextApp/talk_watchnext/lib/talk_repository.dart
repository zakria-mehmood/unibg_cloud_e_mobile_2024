import 'dart:convert';
import 'models/talk.dart';

Future<List<Talk>> initEmptyList() async {
  Iterable list = json.decode("[]");
  var talks = list.map((model) => Talk.fromJSON(model)).toList();
  return talks;
}

Future<List<Talk>> getTalksById(String Id, int page) async {
  // Define the mock data
  final Map<String, List<Talk>> mockData = {
    "521815": [
      Talk(
        watch_next1: "50803",
        speaker1: "TED-Ed",
        watch_next2: "63294",
        speaker2: "Kay Read",
        watch_next3: "83495",
        speaker3: "Ilan Stavans",
        speakers: "Geoffrey E. Braswell",
        title: "The rise and fall of the Maya Empire’s most powerful city",
      ),
    ],
  };

  return mockData[Id] ?? [];
}



