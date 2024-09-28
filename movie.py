class Movie:
    def __init__(self, title="", url_picture="", score="", duration="", description="", director="", genre=""):
        self.__title = title
        self.__url_picture = url_picture
        self.__score = score
        self.__duration = duration
        self.__description = description
        self.__director = director
        self.__genre = genre

    def get_title(self):
        return self.__title
    
    def set_title(self, title):
        self.__title = title

    def get_url_picture(self):
        return self.__url_picture
    
    def set_url_picture(self, url_picture):
        self.__url_picture = url_picture

    def get_score(self):
        return self.__score
    
    def set_score(self, score):
        self.__score = score

    def get_duration(self):
        return self.__duration
    
    def set_duration(self, duration):
        self.__duration = duration

    def get_description(self):
        return self.__description
    
    def set_description(self, description):
        self.__description = description

    def get_director(self):
        return self.__director
    
    def set_director(self, director):
        self.__director = director

    def get_genre(self):
        return self.__genre
    
    def set_genre(self, genre):
        self.__genre = genre

    def display_info(self):
        info = (f"Title: {self.__title}\n"
                f"URL Picture: {self.__url_picture}\n"
                f"Score: {self.__score}\n"
                f"Duration: {self.__duration}\n"
                f"Director: {self.__director}\n"
                f"Genre: {self.__genre}\n"
                f"Description: {self.__description}")
        print(info)
movie = Movie()