import 'package:http/http.dart' as http;
import 'dart:convert';
import 'models/talk.dart';

Future<List<Talk>> initEmptyList() async {

  Iterable list = json.decode("[]");
  var talks = list.map((model) => Talk.fromJSON(model)).toList();
  return talks;

}

Future<List<Talk>> getTalksByTag(String tag, int page) async {
  var url = Uri.parse('https://iekzpiq55k.execute-api.us-east-1.amazonaws.com/default/GetTalkByTag');

  final http.Response response = await http.post(url,
    headers: <String, String>{
      'Content-Type': 'application/json; charset=UTF-8',
    },
    body: jsonEncode(<String, Object>{
      'tag': tag,
      'page': page,
      'doc_per_page': 6
    }),
  );
  if (response.statusCode == 200) {
    Iterable list = json.decode(response.body);
    var talks = list.map((model) => Talk.fromJSON(model)).toList();
    return talks;
  } else {
    throw Exception('Failed to load talks');
  }
      
}