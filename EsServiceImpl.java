package com.elastic.service.impl;


import com.elastic.service.inter.EsService;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class EsServiceImpl implements EsService {

	private static final Logger logger = Logger.getLogger(EsServiceImpl.class);


	@Override
	public long getMaxNum() {
		return 0;
	}

	@Override
	public boolean isEnd(int skip, long maxNum) {
		return skip < maxNum ? false : true;
	}

}
