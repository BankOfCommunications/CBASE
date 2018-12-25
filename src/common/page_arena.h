
#ifndef OCEANBASE_COMMON_PAGE_ARENA_H_
#define OCEANBASE_COMMON_PAGE_ARENA_H_

#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "ob_define.h"
#include "ob_malloc.h"
#include "ob_mod_define.h"
#include "ob_allocator.h"


namespace oceanbase
{
  namespace common
  {
    // convenient function for memory alignment
    inline size_t get_align_offset(void *p)
    {
      size_t size = 0;
      if (sizeof(void *) == 8)
      {
        size = 8 - (((uint64_t)p) & 0x7);
      }
      else
      {
        size = 4 - (((uint64_t)p) & 0x3);
      }
      return size;
    }

    struct DefaultPageAllocator: public ObIAllocator
    {
      DefaultPageAllocator():mod_id_(ObModIds::OB_PAGE_ARENA) {};
      virtual ~DefaultPageAllocator(){};
      void *alloc(const int64_t sz) { return ob_tc_malloc(sz, mod_id_); }
      void free(void *p) { ob_tc_free(p); }
      void freed(const int64_t sz) {UNUSED(sz); /* mostly for effcient bulk stat reporting */ }
      void set_mod_id(int32_t mod_id) {mod_id_ = mod_id;};
      private:
        int32_t mod_id_;
    };

    struct ModulePageAllocator: public ObIAllocator
    {
      explicit ModulePageAllocator(int32_t mod_id = ObModIds::OB_MODULE_PAGE_ALLOCATOR) : mod_id_(mod_id), allocator_(NULL) {}
      explicit ModulePageAllocator(ObIAllocator &allocator) : allocator_(&allocator) {}
      virtual ~ModulePageAllocator(){}
      void set_mod_id(int32_t mod_id) { mod_id_ = mod_id; }
      void *alloc(const int64_t sz) { return (NULL == allocator_) ? ob_tc_malloc(sz, mod_id_) : allocator_->alloc(sz); }
      void free(void *p) { (NULL == allocator_) ? ob_tc_free(p, mod_id_) : allocator_->free(p); }
      void freed(const int64_t sz) {UNUSED(sz); /* mostly for effcient bulk stat reporting */ }
      private:
      int32_t mod_id_;
      ObIAllocator *allocator_;
    };

    /**
     * A simple/fast allocator to avoid individual deletes/frees
     * Good for usage patterns that just:
     * load, use and free the entire container repeatedly.
     */
    template <typename CharT = char, class PageAllocatorT = DefaultPageAllocator>
      class PageArena
      {
        public:
          static const int64_t DEFAULT_PAGE_SIZE = 64*1024; // default 64KB
        private: // types
          typedef PageArena<CharT, PageAllocatorT> Self;

          struct Page
          {
            uint64_t magic_;
            Page *next_page_;
            char *alloc_end_;
            const char *page_end_;
            char buf_[0];

            Page(const char *end) : magic_(0x1234abcddbca4321),next_page_(0), page_end_(end)
            {
              alloc_end_ = buf_;
            }

            int64_t remain() const { return page_end_ - alloc_end_; }
            int64_t used() const { return alloc_end_ - buf_ ; }
            int64_t raw_size() const { return page_end_ - buf_ + sizeof(Page); }

            CharT * alloc(int64_t sz)
            {
              CharT* ret = NULL;
              if (sz <= remain())
              {
                char *start = alloc_end_;
                alloc_end_ += sz;
                ret = (CharT*) start;
              }
              return ret;
            }

            CharT * alloc_down(int64_t sz)
            {
              assert(sz <= remain());
              page_end_ -= sz;
              return (CharT *)page_end_;
            }

            void reuse()
            {
              alloc_end_ = buf_;
            }
          };

        private: // data
          Page *cur_page_;
          Page *header_;
          Page *tailer_;
          int64_t page_limit_;  // capacity in bytes of an empty page
          int64_t page_size_;   // page size in number of bytes
          int64_t pages_;       // number of pages allocated
          int64_t used_;        // total number of bytes allocated by users
          int64_t total_;       // total number of bytes occupied by pages
          PageAllocatorT page_allocator_;

        private: // helpers
          Page *  insert_head(Page *page)
          {
            if (NULL != header_)
            {
              page->next_page_ = header_;
            }
            header_ = page;

            return page;
          }

          Page * insert_tail(Page *page)
          {
            if (NULL != tailer_)
            {
              tailer_->next_page_ = page;
            }
            tailer_ = page;

            return page;
          }

          Page * alloc_new_page(const int64_t sz)
          {
            Page* page = NULL;
            void* ptr = page_allocator_.alloc(sz);

            if (NULL != ptr)
            {
              page  = new (ptr) Page((char *)ptr + sz);

              total_  += sz;
              ++pages_;
            }
            else
            {
              TBSYS_LOG(ERROR, "cannot allocate memory.sz=%ld, pages_=%ld,total_=%ld",
                  sz, pages_, total_);
            }

            return page;
          }

          Page * extend_page(const int64_t sz)
          {
            Page * page = cur_page_;
            if (NULL != page)
            {
              page = page->next_page_;
              if (NULL != page)
              {
                page->reuse();
              }
              else
              {
                page = alloc_new_page(sz);
                if (NULL == page)
                {
                  TBSYS_LOG(ERROR, "extend_page sz =%ld cannot alloc new page", sz);
                }
                insert_tail(page);
              }
            }
            return page;
          }


          inline bool ensure_cur_page()
          {
            if (NULL == cur_page_)
            {
              header_ = cur_page_ = tailer_ = alloc_new_page(page_size_);
              if (NULL != cur_page_)
                page_limit_ = cur_page_->remain();
            }

            return (NULL != cur_page_);
          }

          inline bool is_normal_overflow(const int64_t sz)
          {
            return sz <= page_limit_;
          }

          inline bool is_large_page(Page* page)
          {
            return NULL == page ? false : page->raw_size() > page_size_;
          }

          CharT *alloc_big(const int64_t sz)
          {
            CharT * ptr = NULL;
            // big enough object to have their own page
            Page *p = alloc_new_page(sz + sizeof(Page));
            if (NULL != p)
            {
              insert_head(p);
              ptr = p->alloc(sz);
            }
            return ptr;
          }

          void free_large_pages()
          {
            Page** current = &header_;
            while (NULL != *current)
            {
              Page* entry = *current;
              if (is_large_page(entry))
              {
                *current = entry->next_page_;
                pages_ -= 1;
                total_ -= entry->raw_size();
                page_allocator_.free(entry);
              }
              else
              {
                tailer_ = *current;
                current = &entry->next_page_;
              }

            }
            if (NULL == header_)
            {
              tailer_ = NULL;
            }
          }

          void reset()
          {
            header_ = NULL;
            cur_page_ = NULL;
            tailer_ = NULL;

            pages_ = 0;
            used_ = 0;
            total_ = 0;
          }

          Self & assign(Self & rhs)
          {
            if (this != &rhs)
            {
              free();

              header_ = rhs.header_;
              cur_page_ = rhs.cur_page_;
              tailer_ = rhs.tailer_;

              pages_ = rhs.pages_;
              used_ = rhs.used_;
              total_ = rhs.total_;
              page_size_  = rhs.page_size_;
              page_limit_ = rhs.page_limit_;
              page_allocator_ = rhs.page_allocator_;

            }
            return *this;
          }

        public: // API
          /** constructor */
          PageArena(const int64_t page_size = DEFAULT_PAGE_SIZE,
              const PageAllocatorT &alloc = PageAllocatorT())
            : cur_page_(NULL), header_(NULL), tailer_(NULL),
            page_limit_(0), page_size_(page_size),
            pages_(0), used_(0), total_(0), page_allocator_(alloc)
        {
          assert(page_size > (int64_t)sizeof(Page));
        }
          ~PageArena() { free(); }


          Self & join(Self & rhs)
          {
            if (this != &rhs && rhs.used_ == 0)
            {
              if (NULL == header_)
              {
                assign(rhs);
              }
              else if (NULL != rhs.header_)
              {
                tailer_->next_page_ = rhs.header_;
                tailer_ = rhs.tailer_;

                pages_ += rhs.pages_;
                total_ += rhs.total_;
              }
              rhs.reset();
            }
            return *this;
          }

          int64_t page_size() const { return page_size_; }

          void set_page_size(const int64_t sz)
          {
            assert(sz > (int64_t)sizeof(Page));
            // set page size only in initialized.
            if (NULL == header_ && 0 == total_)
            {
              page_size_ = sz;
            }
          }

          void set_page_alloctor(const PageAllocatorT& alloc)
          {
            page_allocator_ = alloc;
          }

          void set_mod_id(int32_t mod_id) {page_allocator_.set_mod_id(mod_id);};

          /** allocate sz bytes */
          CharT * alloc(const int64_t sz)
          {
            ensure_cur_page();

            // common case
            CharT* ret = NULL;
            if (NULL != cur_page_)
            {
              if (sz <= cur_page_->remain())
              {
                ret = cur_page_->alloc(sz);
              }
              else if (is_normal_overflow(sz))
              {
                cur_page_ = extend_page(page_size_);
                if (NULL != cur_page_)
                {
                  ret = cur_page_->alloc(sz);
                }
              }
              else
              {
                ret = alloc_big(sz);
              }

              if (NULL != ret)
              {
                used_ += sz;
              }
            }
            return ret;
          }

          template<class T>
            T *new_object()
            {
              T *ret = NULL;
              void *tmp = (void *)alloc_aligned(sizeof(T));
              if (NULL == tmp)
              {
                TBSYS_LOG(WARN, "fail to alloc mem for T");
              }
              else
              {
                new(tmp)T();
              }
            }

          /** allocate sz bytes */
          CharT * alloc_aligned(const int64_t sz)
          {
            ensure_cur_page();

            // common case
            CharT* ret = NULL;
            if (NULL != cur_page_)
            {
              int64_t align_offset = get_align_offset(cur_page_->alloc_end_);
              int64_t adjusted_sz = sz + align_offset;

              if (adjusted_sz <= cur_page_->remain())
              {
                used_ += align_offset;
                ret = cur_page_->alloc(adjusted_sz) + align_offset;
              }
              else if (is_normal_overflow(sz))
              {
                cur_page_ = extend_page(page_size_);
                if (NULL != cur_page_)
                {
                  ret = cur_page_->alloc(sz);
                }
              }
              else
              {
                ret = alloc_big(sz);
              }

              if (NULL != ret)
              {
                used_ += sz;
              }
            }
            return ret;
          }

          /**
           * allocate from the end of the page.
           * - allow better packing/space saving for certain scenarios
           */
          CharT* alloc_down(const int64_t sz)
          {
            used_ += sz;
            ensure_cur_page();

            // common case
            CharT* ret = NULL;
            if (NULL != cur_page_)
            {
              if (sz <= cur_page_->remain())
              {
                ret = cur_page_->alloc_down(sz);
              }
              else if (is_normal_overflow(sz))
              {
                cur_page_ = extend_page(page_size_);
                if (NULL != cur_page_)
                {
                  ret = cur_page_->alloc_down(sz);
                }
              }
              else
              {
                ret = alloc_big(sz);
              }
            }
            return ret;
          }

          /** realloc for newsz bytes */
          CharT * realloc(CharT *p, const int64_t oldsz, const int64_t newsz)
          {
            assert(cur_page_);
            CharT* ret = p;
            // if we're the last one on the current page with enough space
            if (p + oldsz == cur_page_->alloc_end_
                && p + newsz  < cur_page_->page_end_)
            {
              cur_page_->alloc_end_ = (char *)p + newsz;
              ret = p;
            }
            else
            {
              ret = alloc(newsz);
              if (NULL != ret)
                ::memcpy(ret, p, newsz > oldsz ? oldsz : newsz);
            }
            return ret;
          }

          /** duplicate a null terminated string s */
          CharT * dup(const char *s)
          {
            if (NULL == s) return NULL;

            int64_t len = strlen(s) + 1;
            CharT *copy = alloc(len);
            if (NULL != copy)
              memcpy(copy, s, len);
            return copy;
          }

          /** duplicate a buffer of size len */
          CharT * dup(const void *s, const int64_t len)
          {
            CharT* copy = NULL;
            if (NULL != s && len > 0)
            {
              copy = alloc(len);
              if (NULL != copy)
                memcpy(copy, s, len);
            }

            return copy;
          }

          /** free the whole arena */
          void free()
          {
            Page *page = NULL;

            while (NULL != header_)
            {
              page = header_;
              header_ = header_->next_page_;
              page_allocator_.free(page);
            }
            page_allocator_.freed(total_);

            cur_page_ = NULL;
            tailer_ = NULL;
            used_ = 0;
            pages_ = 0;
            total_ = 0;
          }

          /**
           * free some of pages. remain memory can be reuse.
           *
           * @param sleep_pages force sleep when pages are freed every time.
           * @param sleep_interval_us sleep interval in microseconds.
           * @param remain_size keep size of memory pages less than %remain_size
           *
           */
          void partial_slow_free(const int64_t sleep_pages,
              const int64_t sleep_interval_us, const int64_t remain_size = 0)
          {
            Page *page = NULL;

            int64_t current_sleep_pages = 0;

            while (NULL != header_ && (remain_size == 0 || total_ > remain_size))
            {
              page = header_;
              header_ = header_->next_page_;

              total_ -= page->raw_size();

              page_allocator_.free(page);

              ++current_sleep_pages;
              --pages_;

              if (sleep_pages > 0 && current_sleep_pages >= sleep_pages)
              {
                ::usleep(static_cast<useconds_t>(sleep_interval_us));
                current_sleep_pages = 0;
              }
            }

            // reset allocate start point, important.
            // once slow_free called, all memory allocated before
            // CANNOT use anymore.
            cur_page_ = header_;
            if (NULL == header_) tailer_ = NULL;
            used_ = 0;
          }

          void free(CharT* ptr)
          {
            UNUSED(ptr);
          }

          void reuse()
          {
            free_large_pages();
            used_ = 0;
            cur_page_ = header_;
            if (NULL != cur_page_)
            {
              cur_page_->reuse();
            }
          }

          /** stats accessors */
          int64_t pages() const { return pages_; }
          int64_t used() const { return used_; }
          int64_t total() const { return total_; }
        private:
          DISALLOW_COPY_AND_ASSIGN(PageArena);
      };

    typedef PageArena<> CharArena;
    typedef PageArena<unsigned char> ByteArena;
    typedef PageArena<char, ModulePageAllocator> ModuleArena;

    class ObArenaAllocator: public ObIAllocator
    {
      public:
        ObArenaAllocator(int32_t mod_id, const int64_t page_size = 64 * 1024)
          :arena_(page_size, ModulePageAllocator(mod_id)){};
        virtual ~ObArenaAllocator(){};
      public:
        virtual void *alloc(const int64_t sz) {return arena_.alloc(sz);};
        virtual void free(void *ptr) {arena_.free(reinterpret_cast<char*>(ptr));};
        void reuse() {arena_.reuse();};
        virtual void set_mod_id(int32_t mod_id) {UNUSED(mod_id);};
        void clear() {arena_.free();};
      private:
        ModuleArena arena_;
    };

#define ALLOC_OBJECT(allocator, obj, T, arg...) \
    do \
    { \
      obj = NULL; \
      void *tmp = reinterpret_cast<void *>(allocator.alloc_aligned(sizeof(T))); \
      if (NULL == tmp) \
      { \
        TBSYS_LOG(WARN, "alloc mem for %s", #T); \
      } \
      else \
      { \
        obj = new(tmp) T(##arg); \
      } \
    } \
    while (0)

  } // end namespace common
} // end namespace oceanbase

#endif // end if OCEANBASE_COMMON_PAGE_ARENA_H_
