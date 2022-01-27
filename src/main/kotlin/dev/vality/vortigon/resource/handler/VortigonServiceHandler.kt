package dev.vality.vortigon.resource.handler

import dev.vality.damsel.domain.CategoryType
import dev.vality.damsel.vortigon.PartyFilterRequest
import dev.vality.damsel.vortigon.PaymentInstitutionRealm
import dev.vality.damsel.vortigon.VortigonServiceSrv
import dev.vality.vortigon.domain.db.tables.pojos.Party
import dev.vality.vortigon.repository.PartyDao
import dev.vality.vortigon.repository.ShopDao
import dev.vality.vortigon.repository.model.ContractFilter
import dev.vality.vortigon.repository.model.ShopFilter
import mu.KotlinLogging
import org.springframework.stereotype.Component
import java.util.stream.Collectors
import dev.vality.vortigon.repository.model.PartyFilter as PartyFilterRepo

private val log = KotlinLogging.logger {}

@Component
class VortigonServiceHandler(
    private val partyDao: PartyDao,
    private val shopDao: ShopDao
) : VortigonServiceSrv.Iface {

    override fun getShopsIds(partyId: String, paymentInstitutionRealm: PaymentInstitutionRealm): MutableList<String> {
        log.debug("-> get shops ids by partyId: $partyId env: $paymentInstitutionRealm")
        val shopIds = shopDao.findShopIdsByPartyIdAndCategoryType(partyId, resolveCategoryType(paymentInstitutionRealm))
        log.debug("-> get shops ids by partyId: $partyId env: $paymentInstitutionRealm result: $shopIds")
        return shopIds
    }

    override fun findPartyIds(filter: PartyFilterRequest): MutableList<String> {
        log.info("Find party ids. filter={}", filter)
        val partyFilter = convertFilter(filter)
        return partyDao.getPartyByFilter(partyFilter).stream()
            .map(Party::getPartyId)
            .collect(Collectors.toList())
    }

    private fun resolveCategoryType(paymentInstitutionRealm: PaymentInstitutionRealm): String {
        return when (paymentInstitutionRealm) {
            PaymentInstitutionRealm.live -> CategoryType.live.name
            PaymentInstitutionRealm.test -> CategoryType.test.name
            else -> throw IllegalArgumentException("resolveCategoryType environment: $paymentInstitutionRealm is unknown!")
        }
    }

    private fun convertFilter(partyFilterRequest: PartyFilterRequest): PartyFilterRepo {
        val shopFilter = if (partyFilterRequest.isSetShopFilter) {
            ShopFilter().apply {
                if (partyFilterRequest.shopFilter != null) {
                    locationUrl = partyFilterRequest.shopFilter.locationUrl
                }
                if (partyFilterRequest.shopFilter != null && partyFilterRequest.shopFilter.category_filter != null) {
                    categoryName = partyFilterRequest.shopFilter.category_filter.name
                }
            }
        } else null
        val contractFilter = if (partyFilterRequest.isSetContractFilter) {
            ContractFilter().apply {
                if (partyFilterRequest.contractFilter != null) {
                    legalAgreementSignedAt = partyFilterRequest.contract_filter.legal_agreement_signed_at
                }
            }
        } else null

        return PartyFilterRepo().apply {
            email = partyFilterRequest.party_filter.contact_info_email
            this.shopFilter = shopFilter
            this.contractFilter = contractFilter
        }
    }
}
