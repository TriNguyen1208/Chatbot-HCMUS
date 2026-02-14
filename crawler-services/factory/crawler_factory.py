from services.crawler_base_services import CrawlerBaseServices
from services.crawler_announcement_services import CrawlerAnnouncementServices
from services.crawler_curriculum_services import CrawlerCurriculumServices
from services.crawler_enrollment_services import CrawlerEnrollmentServices
from services.crawler_fitInfo_services import CrawlerFitInfoServices

class CrawlerFactory:
    @staticmethod
    def get_crawler(name: str):
        crawlers: dict[str, CrawlerBaseServices] = {
            "curriculum": CrawlerCurriculumServices, #https://www.ctda.hcmus.edu.vn/vi/educational-program/
            "fit_info": CrawlerFitInfoServices, #https://www.fit.hcmus.edu.vn/ (lấy thông tin khoa)
            "announcement": CrawlerAnnouncementServices, # https://hcmus.edu.vn/category/dao-tao/dai-hoc/thong-tin-danh-cho-sinh-vien/ 
            "enrollment": CrawlerEnrollmentServices #https://tuyensinh.hcmus.edu.vn/ (lấy hết của cntt)
        }
        crawler_instance = crawlers.get(name)
        if not crawler_instance:
            raise ValueError("Name of crawler is not existed")
        return crawler_instance()