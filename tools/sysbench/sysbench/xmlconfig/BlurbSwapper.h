/**
 * 
 * @file  BlurbSwapper.h 
 * @author  gaoxinbo <xinbo.gaoxb@alibaba-inc.com>
 * 
 * @version 1.0
 * @data 2011-10-01
 * 
 * Copyright(C) 2011 china.alibaba.com all rights reserved.
 *
 **/


#include <simon_iProcess.h>
#include <sys/time.h>

namespace iprocess{
    class CBlurbSwapper{
        public: 
            explicit CBlurbSwapper(simon::PluginBlurbBlurbC * blurb):m_blurb(blurb){
                blurb->incrInCount(1);
                gettimeofday(&start, NULL);
            }
            ~CBlurbSwapper(){                
                if(m_blurb!=NULL){
                    gettimeofday(&end, NULL);
                    m_blurb->setProcessLatency((end.tv_sec - start.tv_sec)*1000000 + end.tv_usec - start.tv_usec);
                    m_blurb->update();
                }
            }
        private:
			timeval start,end;
            simon::PluginBlurbBlurbC * m_blurb; 
    };
}
