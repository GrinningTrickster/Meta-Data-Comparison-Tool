import re


def cleaner(string):
    string = re.sub(' +', ' ', string)
    string = string.replace('\n', '').replace('\r', '').replace('\t', '')
    return string


def get_title(soup):
    try:
        title = soup.find('title')
        if title is not None:
            title = title.get_text()
            title = cleaner(title)
    except:
        title = ''
    return title


def get_description(soup):
    metadescription = re.compile('Description|description|DESCRIPTION')
    try:
        description = soup.find('meta', attrs={'name': metadescription})
        if description is not None:
            description = description['content']
            description = cleaner(description)
        else:
            description = ''
    except:
        description = ''
    return description


def get_h1(soup):
    h1 = ""
    try:
        h1tag = soup.find('h1')
        if h1tag is not None:
            h1 = h1tag.text.strip()
            h1 = cleaner(h1)
    except:
        h1 = ''
    return h1













