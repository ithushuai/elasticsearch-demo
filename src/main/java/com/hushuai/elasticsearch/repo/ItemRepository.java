package com.hushuai.elasticsearch.repo;

import com.hushuai.elasticsearch.pojo.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

/**
 * created by it_hushuai
 * 2020/2/18 22:44
 */
public interface ItemRepository extends ElasticsearchRepository<Item, Long> {
}
