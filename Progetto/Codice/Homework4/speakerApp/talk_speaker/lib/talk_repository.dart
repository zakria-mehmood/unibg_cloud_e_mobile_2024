import 'dart:convert';
import 'models/talk.dart';

Future<List<Talk>> initEmptyList() async {
  Iterable list = json.decode("[]");
  var talks = list.map((model) => Talk.fromJson(model)).toList();
  return talks;
}

Future<List<Talk>> getTalksBySpeaker(String Speaker, int page) async {

  final Map<String, List<Talk>> mockData = {
    "Kristen Bell + Giant Ant": [
      Talk(
        id: 357705,
        speakers: "Kristen Bell + Giant Ant",
        title: "Why act now?",
        url: "https://www.ted.com/talks/kristen_bell_giant_ant_why_act_now",
        duration: 82,
      ),
      Talk(
        id: 357749,
        speakers: "Kristen Bell + Giant Ant",
        title: "Where does all the carbon we release go?",
        url: "https://www.ted.com/talks/kristen_bell_giant_ant_where_does_all_the_ca…",
        duration: 83,
      ),
      Talk(
        id: 357703,
        speakers: "Kristen Bell + Giant Ant",
        title: "Why is 1.5 degrees such a big deal?",
        url: "https://www.ted.com/talks/kristen_bell_giant_ant_why_is_1_5_degrees_su…",
        duration: 70,
      ),
      Talk(
        id: 357699,
        speakers: "Kristen Bell + Giant Ant",
        title: "What is net-zero?",
        url: "https://www.ted.com/talks/kristen_bell_giant_ant_what_is_net_zero",
        duration: 73,
      ),
      Talk(
        id: 357697,
        speakers: "Kristen Bell + Giant Ant",
        title: "Why is the world warming up?",
        url: "https://www.ted.com/talks/kristen_bell_giant_ant_why_is_the_world_warm…",
        duration: 76,
      ),
    ],
    "Alain de Botton": [
      Talk(
        id: 64122,
        speakers: "Alain de Botton",
        title: "Atheism 2.0",
        url: "https://www.ted.com/talks/alain_de_botton_atheism_2_0",
        duration: 997,
      ),
      Talk(
        id: 26754,
        speakers: "Alain de Botton",
        title: "A kinder, gentler philosophy of success",
        url: "https://www.ted.com/talks/alain_de_botton_a_kinder_gentler_philosophy_of_success",
        duration: 997,
      ),
    ],
  };

  return mockData[Speaker] ?? [];
}
