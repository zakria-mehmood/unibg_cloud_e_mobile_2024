class Talk {
  final String speakers;
  final String title;
  final String watch_next1;
  final String speaker1;
  final String watch_next2;
  final String speaker2;
  final String watch_next3;
  final String speaker3;

  Talk({
    required this.speakers,
    required this.title,
    required this.watch_next1,
    required this.speaker1,
    required this.watch_next2,
    required this.speaker2,
    required this.watch_next3,
    required this.speaker3,
  });

  factory Talk.fromJSON(Map<String, dynamic> json) {
    return Talk(
      speakers: json['speakers'],
      title: json['title'],
      watch_next1: json['watch_next1'],
      speaker1: json['speaker1'],
      watch_next2: json['watch_next2'],
      speaker2: json['speaker2'],
      watch_next3: json['watch_next3'],
      speaker3: json['speaker3'],
    );
  }

  Map<String, dynamic> toJSON() {
    return {
      'speakers': speakers,
      'title': title,
      'watch_next1': watch_next1,
      'speaker1': speaker1,
      'watch_next2': watch_next2,
      'speaker2': speaker2,
      'watch_next3': watch_next3,
      'speaker3': speaker3,
    };
  }

  String get details => 
      'Speakers: $speakers\n'
      'Title: $title\n'
      'Watch Next 1: $watch_next1 - $speaker1\n'
      'Watch Next 2: $watch_next2 - $speaker2\n'
      'Watch Next 3: $watch_next3 - $speaker3';
}
