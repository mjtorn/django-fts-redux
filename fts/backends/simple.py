"Simple Fts backend"
import os
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic
from django.db.models import Q

from django.db import transaction

from fts.backends.base import BaseClass, BaseModel, BaseManager
from fts.models import Word, Index

from fts.words.stop import FTS_STOPWORDS

try:
    from fts.words.snowball import Stemmer
except ImportError:
    from fts.words.porter import Stemmer

WEIGHTS = {
    'A' : 10,
    'B' : 4,
    'C' : 2,
    'D' : 1
}

class SearchClass(BaseClass):
    def __init__(self, server, params):
        self.backend = 'simple'

class SearchManager(BaseManager):
    @transaction.commit_on_success
    def update_index(self, pk=None):
        if pk is not None:
            if isinstance(pk, (list,tuple)):
                items = self.filter(pk__in=pk)
            else:
                items = self.filter(pk=pk)
            items[0]._index.all().delete()
        else:
            items = self.all()
            ctype = ContentType.objects.get_for_model(self.model)
            Index.objects.filter(content_type__pk=ctype.id).delete()
        
        IW = {}
        for item in items:
            for field, weight in self._fields.items():
                for word in set(getattr(item, field).lower().split(' ')):
                    if word and word not in FTS_STOPWORDS[self.language_code]:
                        p = Stemmer(self.language_code)
                        word = p(word)
                        try:
                            iw = IW[word];
                        except KeyError:
                            iw = Word.objects.get_or_create(word=word)[0]
                            IW[w] = iw
                        i = Index(content_object=item, word=iw, weight=WEIGHTS[weight])
                        i.save()

    def search(self, query, **kwargs):
        '''
        params = Q()
        #SELECT core_blog.*, count(DISTINCT word_id), sum(weight)
        #FROM core_blog INNER JOIN fts_index ON (core_blog.id = fts_index.object_id) INNER JOIN fts_word ON (fts_index.word_id = fts_word.id)
        #WHERE fts_index.content_type_id = 18  AND (fts_word.word='titl' OR fts_word.word='simpl')
        #GROUP BY core_blog.id, core_blog.title, core_blog.body
        #HAVING count(DISTINCT word_id) = 2;
        words = 0
        for w in set(query.lower().split(' ')):
            if w and w not in FTS_STOPWORDS[self.language_code]:
                words += 1
                p = Stemmer(self.language_code)
                w = p(w)
                params |= Q(_index__word__word=w)
        qs = self.filter(params)
        #if words > 1:
        #    qs.query.group_by = ['core_blog.id, core_blog.title, core_blog.body']
        #    qs.query.having = ['(COUNT(DISTINCT fts_index.word_id)) = %d' % words]
        return qs.distinct()
        '''
        #SELECT %(table_name)s.* FROM %(table_name)s
        #    INNER JOIN %(index_table_name)s AS w1 ON (w1.word LIKE 'word1%') INNER JOIN %(index_table_name)s AS i1 ON (w1.id = i1.word_id AND i1.content_type_id = %(content_type_id)s AND i1.object_id = %(table_name)s.id)
        #    INNER JOIN %(index_table_name)s AS w2 ON (w2.word LIKE 'word2%') INNER JOIN %(index_table_name)s AS i2 ON (w2.id = i2.word_id AND i2.content_type_id = %(content_type_id)s AND i2.object_id = %(table_name)s.id);
        qs = self.all()
        joins = []
        joins_params = []
        words = 0
        for word in set(query.lower().split(' ')):
            if word and word not in FTS_STOPWORDS[self.language_code]:
                words += 1
                p = Stemmer(self.language_code)
                word = p(word)
                joins.append("INNER JOIN %%(words_table_name)s AS w%(words)d ON (w%(words)d.word LIKE '%%%%s%%%%%%%%') INNER JOIN %%(index_table_name)s AS i%(words)d ON (w%(words)d.id = i%(words)d.word_id AND i%(words)d.content_type_id = %%(content_type_id)s AND i%(words)d.object_id = %%(table_name)s.id)" % { 'words':words })
                joins_params.append(word)
        
        table_name = self.model._meta.db_table
        ctype = ContentType.objects.get_for_model(self.model)
        joins = ' '.join(joins) % {
            'table_name': qs.query.quote_name_unless_alias(table_name),
            'words_table_name': qs.query.quote_name_unless_alias(Word._meta.db_table),
            'index_table_name': qs.query.quote_name_unless_alias(Index._meta.db_table),
            'content_type_id': ctype.id,
        }
        # these params should be set as form params to be returned by get_from_clause() but it doesn't support form params
        joins = joins % tuple(joins_params)
        # monkey patch the query set:
        qs.query.table_alias(table_name) # create alias
        qs.query.alias_map[table_name] = (table_name, joins, None, None, None, None, None) # map the joins to the alias
        return qs

class SearchableModel(BaseModel):
    class Meta:
        abstract = True

    _index = generic.GenericRelation(Index)

    objects = SearchManager()
