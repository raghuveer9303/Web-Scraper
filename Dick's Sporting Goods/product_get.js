const axios = require('axios');

async function getProducts(searchTerm) {
    const baseUrl = "https://prod-catalog-product-api.dickssportinggoods.com/v2/search";
    
    // Constructing the searchVO parameter
    const searchVO = {
        pageNumber: 0,
        pageSize: 144,
        selectedSort: 0,
        selectedStore: "3117",
        storeId: "15108",
        zipcode: "46250",
        isFamilyPage: false,
        mlBypass: false,
        snbAudience: "",
        searchTerm: searchTerm
    };

    try {
        // Making the GET request
        const response = await axios.get(baseUrl, {
            params: {
                searchVO: JSON.stringify(searchVO)
            }
        });

        if (response.status === 200) {
            const data = response.data;
            
            // Extract product data
            const products = data.productVOs || [];
            const result = products.map(product => [product.partnumber, product.name]);

            return result;
        }
    } catch (error) {
        console.error(`Error: ${error.response?.status}, Message: ${error.message}`);
    }
    
    return [];
}

module.exports = { getProducts };