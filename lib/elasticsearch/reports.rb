require "elasticsearch/reports/version"

module Elasticsearch
  module Reports
    included do
      def self.query_hash(search_query)
        query_terms     =  []
        daterange_list  = search_query[:daterange_list]
        set_range_filters(daterange_list || [], query_terms)
        ::ES_QUERY_PARAMS.each do |field, param|
          query_terms << {terms: {field => [search_query[param]].flatten}} if search_query[param].present?
        end
        query_terms
      end

      def self.set_range_filters(daterange_list, query_terms)
        daterange_list.select!{|field, val| val[:range_value].present?}
        daterange_list.each do |field, range|
          from, to = range[:range_value].split(/ - /)
          query_terms << {range: {field => {gte: from.to_date, lte: to.to_date}}}
        end
      end
      
      def self.search(search_query)
        search_params = check_for_query_errors(search_query)
        __elasticsearch__.search(
          {
            size: search_query[:limit] || __elasticsearch__.search({}).total,
            query: {
              filtered: search_params
            }
          }.merge(::ES_SORT_PARAMS)
        )
      end

      def self.check_for_query_errors(search_query)
        search_query.map{|key,value| value.reject!(&:blank?) if value.is_a?(Array)}
        search_params = { 
          filter: {
            'and' => self.query_hash(search_query)
          }
        }

        if search_query[:refine_search].present?
          search_params[:query] = {
            simple_query_string: {
              query: search_query[:refine_search],
              default_operator: 'and'
            }
          }
        end

        search_params
      end
    end
  end
end
