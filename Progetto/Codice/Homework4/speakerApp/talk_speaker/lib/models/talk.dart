class Talk {
  final int id;
  final String speakers;
  final String title;
  final String url;
  final int duration;

  Talk({
    required this.id,
    required this.speakers,
    required this.title,
    required this.url,
    required this.duration,
  });

  factory Talk.fromJson(Map<String, dynamic> json) {
    return Talk(
      id: json['id'],
      speakers: json['speakers'],
      title: json['title'],
      url: json['url'],
      duration: json['duration'],
    );
  }
}
